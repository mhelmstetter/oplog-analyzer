package com.mongodb.oploganalyzer;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command to analyze oplog dump files and detect workload patterns
 */
@Command(name = "analyze", description = "Analyze oplog dump files to detect workload patterns and shard key distribution issues")
public class AnalyzeCommand implements Callable<Integer> {
    
    private static final Logger logger = LoggerFactory.getLogger(AnalyzeCommand.class);
    
    @Parameters(description = "BSON dump files to analyze (.bson or .bson.gz)")
    private List<String> inputFiles = new ArrayList<>();
    
    @Option(names = {"--pattern"}, description = "File pattern to match in current directory (e.g., '*.bson', 'oplog_*.bson.gz')", defaultValue = "*.bson*")
    private String filePattern = "*.bson*";
    
    @Option(names = {"--shardKeyAnalysis"}, description = "Perform detailed shard key distribution analysis", defaultValue = "true")
    private boolean shardKeyAnalysis = true;
    
    @Option(names = {"--workloadGrouping"}, description = "Group and classify workload patterns", defaultValue = "true") 
    private boolean workloadGrouping = true;
    
    // Analysis results
    private final Map<String, ShardWorkload> shardWorkloads = new HashMap<>();
    private final Map<String, ShardKeyInfo> shardKeys = new HashMap<>();
    private final Map<String, CollectionPattern> collectionPatterns = new HashMap<>();
    
    static class ShardWorkload {
        String shardId;
        long totalOps = 0;
        long totalBytes = 0;
        double avgBytesPerOp = 0;
        Map<String, NamespaceStats> namespaceStats = new HashMap<>();
        
        static class NamespaceStats {
            long opCount = 0;
            long totalBytes = 0;
            long minSize = Long.MAX_VALUE;
            long maxSize = Long.MIN_VALUE;
            String dominantOpType = "";
        }
    }
    
    static class ShardKeyInfo {
        String namespace;
        BsonDocument shardKey;
        boolean isHashed = false;
        String keyFields = "";
    }
    
    static class CollectionPattern {
        String namespace;
        Map<String, Long> opTypeCounts = new HashMap<>();
        long totalSize = 0;
        double avgSize = 0;
        String pattern = ""; // "heavy-updates", "frequent-small", "mixed", etc.
    }
    
    @Override
    public Integer call() {
        try {
            if (inputFiles.isEmpty()) {
                // Auto-discover files in current directory
                discoverFiles();
            }
            
            if (inputFiles.isEmpty()) {
                System.err.println("No BSON files found to analyze. Use --pattern to specify file pattern.");
                return 1;
            }
            
            System.out.printf("Analyzing %d BSON files...%n", inputFiles.size());
            
            for (String fileName : inputFiles) {
                System.out.printf("Processing: %s%n", fileName);
                analyzeFile(fileName);
            }
            
            generateAnalysisReport();
            return 0;
            
        } catch (Exception e) {
            logger.error("Error during analysis", e);
            System.err.println("Analysis failed: " + e.getMessage());
            return 1;
        }
    }
    
    private void discoverFiles() {
        File currentDir = new File(".");
        File[] files = currentDir.listFiles((dir, name) -> {
            String pattern = filePattern.replace("*", ".*");
            return name.matches(pattern);
        });
        
        if (files != null) {
            for (File file : files) {
                inputFiles.add(file.getName());
            }
        }
    }
    
    private void analyzeFile(String fileName) throws IOException {
        InputStream inputStream = new FileInputStream(fileName);
        
        // Handle gzipped files
        if (fileName.endsWith(".gz")) {
            inputStream = new GZIPInputStream(inputStream);
        }
        
        inputStream = new BufferedInputStream(inputStream);
        
        try {
            while (true) {
                RawBsonDocument doc = readNextDocument(inputStream);
                if (doc == null) break;
                
                processDocument(doc);
            }
        } finally {
            inputStream.close();
        }
    }
    
    private RawBsonDocument readNextDocument(InputStream inputStream) throws IOException {
        // Read BSON document length (first 4 bytes)
        byte[] lengthBytes = new byte[4];
        int bytesRead = inputStream.read(lengthBytes);
        if (bytesRead != 4) return null; // EOF
        
        int length = ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (length < 5 || length > 16 * 1024 * 1024) { // Basic sanity check
            throw new IOException("Invalid BSON document length: " + length);
        }
        
        // Read the rest of the document
        byte[] docBytes = new byte[length];
        System.arraycopy(lengthBytes, 0, docBytes, 0, 4);
        
        int remaining = length - 4;
        int offset = 4;
        while (remaining > 0) {
            int read = inputStream.read(docBytes, offset, remaining);
            if (read == -1) throw new IOException("Unexpected EOF while reading BSON document");
            offset += read;
            remaining -= read;
        }
        
        return new RawBsonDocument(docBytes);
    }
    
    private void processDocument(RawBsonDocument doc) {
        try {
            // Check if this is a config.collections document (shard key metadata)
            BsonValue nsValue = doc.get("_id");
            if (nsValue != null && nsValue instanceof BsonString) {
                String namespace = nsValue.asString().getValue();
                if (doc.containsKey("key")) {
                    // This is a config.collections document
                    processShardKeyMetadata(namespace, doc);
                    return;
                }
            }
            
            // Check if this is an oplog entry
            BsonValue opValue = doc.get("op");
            if (opValue instanceof BsonString) {
                processOplogEntry(doc);
            }
            
        } catch (Exception e) {
            logger.debug("Error processing document: {}", e.getMessage());
        }
    }
    
    private void processShardKeyMetadata(String namespace, RawBsonDocument doc) {
        try {
            BsonDocument keyDoc = doc.getDocument("key");
            boolean isHashed = doc.getBoolean("unique", org.bson.BsonBoolean.FALSE).getValue();
            
            ShardKeyInfo info = new ShardKeyInfo();
            info.namespace = namespace;
            info.shardKey = keyDoc;
            info.isHashed = isHashed;
            info.keyFields = keyDoc.keySet().toString();
            
            shardKeys.put(namespace, info);
            System.out.printf("Shard key for %s: %s%n", namespace, info.keyFields);
            
        } catch (Exception e) {
            logger.debug("Error processing shard key metadata for {}: {}", namespace, e.getMessage());
        }
    }
    
    private void processOplogEntry(RawBsonDocument doc) {
        try {
            String opType = doc.getString("op").getValue();
            String namespace = doc.getString("ns").getValue();
            
            if (namespace.startsWith("config.")) {
                return; // Skip config operations
            }
            
            long docSize = doc.getByteBuffer().remaining();
            
            // Extract shard information from the document context
            // For now, we'll infer shard from file name or use "unknown"
            String shardId = "unknown"; // TODO: Extract from file name or document context
            
            // Update shard workload
            ShardWorkload workload = shardWorkloads.computeIfAbsent(shardId, k -> {
                ShardWorkload w = new ShardWorkload();
                w.shardId = k;
                return w;
            });
            
            workload.totalOps++;
            workload.totalBytes += docSize;
            workload.avgBytesPerOp = (double) workload.totalBytes / workload.totalOps;
            
            // Update namespace stats within this shard
            ShardWorkload.NamespaceStats nsStats = workload.namespaceStats.computeIfAbsent(namespace, 
                k -> new ShardWorkload.NamespaceStats());
            
            nsStats.opCount++;
            nsStats.totalBytes += docSize;
            nsStats.minSize = Math.min(nsStats.minSize, docSize);
            nsStats.maxSize = Math.max(nsStats.maxSize, docSize);
            nsStats.dominantOpType = opType; // Simplified - could track all op types
            
            // Update collection patterns
            CollectionPattern pattern = collectionPatterns.computeIfAbsent(namespace, k -> {
                CollectionPattern p = new CollectionPattern();
                p.namespace = k;
                return p;
            });
            
            pattern.opTypeCounts.merge(opType, 1L, Long::sum);
            pattern.totalSize += docSize;
            // avgSize will be calculated later
            
        } catch (Exception e) {
            logger.debug("Error processing oplog entry: {}", e.getMessage());
        }
    }
    
    private void generateAnalysisReport() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("WORKLOAD PATTERN ANALYSIS");
        System.out.println("=".repeat(80));
        
        if (shardKeyAnalysis && !shardKeys.isEmpty()) {
            printShardKeyAnalysis();
        }
        
        if (workloadGrouping && !shardWorkloads.isEmpty()) {
            printWorkloadGrouping();
        }
        
        printCollectionPatterns();
        printRecommendations();
    }
    
    private void printShardKeyAnalysis() {
        System.out.println("\n--- SHARD KEY ANALYSIS ---");
        
        for (ShardKeyInfo info : shardKeys.values()) {
            System.out.printf("Collection: %s%n", info.namespace);
            System.out.printf("  Shard Key: %s%n", info.keyFields);
            System.out.printf("  Hashed: %s%n", info.isHashed ? "Yes" : "No");
            
            // Analyze potential issues
            if (info.keyFields.contains("_id") && !info.isHashed) {
                System.out.println("  ⚠️  WARNING: _id shard key without hashing may cause hotspots");
            }
            if (info.keyFields.split(",").length == 1) {
                System.out.println("  ⚠️  WARNING: Single-field shard key may not distribute evenly");
            }
            System.out.println();
        }
    }
    
    private void printWorkloadGrouping() {
        System.out.println("\n--- WORKLOAD GROUPS ---");
        
        // Classify shards by operation characteristics
        List<ShardWorkload> heavyOpsShards = new ArrayList<>();
        List<ShardWorkload> lightOpsShards = new ArrayList<>();
        
        for (ShardWorkload workload : shardWorkloads.values()) {
            if (workload.avgBytesPerOp > 2000) { // >2KB average
                heavyOpsShards.add(workload);
            } else {
                lightOpsShards.add(workload);
            }
        }
        
        if (!heavyOpsShards.isEmpty()) {
            System.out.println("\n🔴 HIGH-IMPACT SHARDS (Large operations, cache-intensive):");
            heavyOpsShards.stream()
                .sorted(Comparator.comparingDouble((ShardWorkload w) -> w.avgBytesPerOp).reversed())
                .forEach(w -> System.out.printf("  %s: %,d ops, %.1f KB/op, %s total%n",
                    w.shardId, w.totalOps, w.avgBytesPerOp / 1024, 
                    org.apache.commons.io.FileUtils.byteCountToDisplaySize(w.totalBytes)));
        }
        
        if (!lightOpsShards.isEmpty()) {
            System.out.println("\n🟡 HIGH-FREQUENCY SHARDS (Many small operations):");
            lightOpsShards.stream()
                .sorted(Comparator.comparingLong((ShardWorkload w) -> w.totalOps).reversed())
                .forEach(w -> System.out.printf("  %s: %,d ops, %.1f KB/op, %s total%n",
                    w.shardId, w.totalOps, w.avgBytesPerOp / 1024,
                    org.apache.commons.io.FileUtils.byteCountToDisplaySize(w.totalBytes)));
        }
    }
    
    private void printCollectionPatterns() {
        System.out.println("\n--- COLLECTION PATTERNS ---");
        
        // Calculate averages and classify patterns
        for (CollectionPattern pattern : collectionPatterns.values()) {
            long totalOps = pattern.opTypeCounts.values().stream().mapToLong(Long::longValue).sum();
            if (totalOps > 0) {
                pattern.avgSize = (double) pattern.totalSize / totalOps;
                
                // Classify pattern
                if (pattern.avgSize > 5000) {
                    pattern.pattern = "heavy-updates";
                } else if (totalOps > 10000 && pattern.avgSize < 1000) {
                    pattern.pattern = "frequent-small";
                } else {
                    pattern.pattern = "mixed";
                }
            }
        }
        
        // Sort by total size impact
        collectionPatterns.values().stream()
            .sorted(Comparator.comparingLong((CollectionPattern p) -> p.totalSize).reversed())
            .forEach(pattern -> {
                long totalOps = pattern.opTypeCounts.values().stream().mapToLong(Long::longValue).sum();
                System.out.printf("\n📊 %s (%s)%n", pattern.namespace, pattern.pattern);
                System.out.printf("  Total operations: %,d%n", totalOps);
                System.out.printf("  Average size: %.1f KB%n", pattern.avgSize / 1024);
                System.out.printf("  Total impact: %s%n", 
                    org.apache.commons.io.FileUtils.byteCountToDisplaySize(pattern.totalSize));
                
                // Operation type breakdown
                System.out.printf("  Operations: ");
                pattern.opTypeCounts.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .forEach(entry -> System.out.printf("%s=%,d ", entry.getKey(), entry.getValue()));
                System.out.println();
                
                // Analysis
                if ("heavy-updates".equals(pattern.pattern)) {
                    System.out.println("  🔴 HIGH CACHE IMPACT: Large updates stress cache and I/O");
                } else if ("frequent-small".equals(pattern.pattern)) {
                    System.out.println("  🟡 HIGH THROUGHPUT: Many small ops stress CPU and locking");
                }
            });
    }
    
    private void printRecommendations() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("OPTIMIZATION RECOMMENDATIONS");
        System.out.println("=".repeat(80));
        
        // Analyze shard distribution
        if (shardWorkloads.size() > 1) {
            double maxAvgSize = shardWorkloads.values().stream()
                .mapToDouble(w -> w.avgBytesPerOp).max().orElse(0);
            double minAvgSize = shardWorkloads.values().stream()
                .mapToDouble(w -> w.avgBytesPerOp).min().orElse(0);
            
            if (maxAvgSize > minAvgSize * 2) {
                System.out.println("\n🔧 SHARD KEY OPTIMIZATION:");
                System.out.printf("   Detected %.1fx size skew between shards (%.1f KB vs %.1f KB avg/op)%n",
                    maxAvgSize / minAvgSize, maxAvgSize / 1024, minAvgSize / 1024);
                System.out.println("   Consider:");
                System.out.println("   • Adding high-cardinality field to shard key");
                System.out.println("   • Using hashed shard key for even distribution");
                System.out.println("   • Splitting large documents into smaller chunks");
            }
        }
        
        // Collection-specific recommendations
        System.out.println("\n🔧 COLLECTION OPTIMIZATIONS:");
        
        for (CollectionPattern pattern : collectionPatterns.values()) {
            if ("heavy-updates".equals(pattern.pattern)) {
                System.out.printf("   %s: Consider document bucketing or field-level updates%n", 
                    pattern.namespace);
            } else if ("frequent-small".equals(pattern.pattern)) {
                System.out.printf("   %s: Consider batching or aggregation pipeline updates%n",
                    pattern.namespace);
            }
        }
        
        // Shard key recommendations
        if (!shardKeys.isEmpty()) {
            System.out.println("\n🔧 SHARD KEY RECOMMENDATIONS:");
            for (ShardKeyInfo info : shardKeys.values()) {
                CollectionPattern pattern = collectionPatterns.get(info.namespace);
                if (pattern != null && "heavy-updates".equals(pattern.pattern)) {
                    System.out.printf("   %s: Current key %s may not distribute large docs evenly%n",
                        info.namespace, info.keyFields);
                }
            }
        }
    }
    
}