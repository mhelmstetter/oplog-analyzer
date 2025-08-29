package com.mongodb.oploganalyzer;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.bson.BsonUuidUtil;
import com.mongodb.util.bson.BsonValueConverter;

import picocli.CommandLine.Option;

/**
 * Base class for oplog analysis commands with shared options and functionality
 */
public abstract class BaseOplogCommand implements Callable<Integer> {
    
    protected static Logger logger = LoggerFactory.getLogger(BaseOplogCommand.class);
    
    // Connection options
    @Option(names = {"-c", "--uri"}, description = "MongoDB connection string URI", required = false)
    protected String uri;
    
    @Option(names = {"-f", "--file"}, description = "BSON or gzipped BSON file to analyze (.bson or .bson.gz)")
    protected String inputFile;
    
    // Threshold options
    @Option(names = {"-t", "--threshold"}, description = "Log operations >= this size (bytes)", defaultValue = "9223372036854775807")
    protected Long threshold = Long.MAX_VALUE;
    
    // ID Statistics options
    @Option(names = {"--idStats"}, description = "Enable _id statistics tracking")
    protected boolean idStats = false;
    
    @Option(names = {"--idStatsThreshold"}, description = "Minimum size for documents to include in ID statistics", defaultValue = "0")
    protected long idStatsThreshold = 0;
    
    @Option(names = {"--topIdCount"}, description = "Number of top frequent _id values to report", defaultValue = "20")
    protected int topIdCount = 20;
    
    @Option(names = {"--fetchDocSizes"}, description = "Fetch actual document sizes for updates (slower but accurate)")
    protected boolean fetchDocSizes = false;
    
    // Shard options
    @Option(names = {"--shardIndex"}, description = "Comma-separated list of shard indices to analyze (0,1,2...), default: all shards")
    protected String shardIndexes;
    
    // Shared data structures
    protected Map<OplogEntryKey, EntryAccumulator> accumulators = new ConcurrentHashMap<>();
    protected IdStatisticsManager idStatisticsManager;
    protected ShardClient shardClient;
    protected boolean stop = false;
    
    /**
     * Initialize common components
     */
    protected void initializeCommon() {
        // Initialize ID statistics manager if enabled
        if (idStats) {
            idStatisticsManager = new IdStatisticsManager(idStatsThreshold, topIdCount);
        }
        
        // Initialize ShardClient if URI is provided
        if (uri != null) {
            shardClient = new ShardClient("oplog-analyzer", uri);
            shardClient.init();
            shardClient.populateShardMongoClients();
        }
    }
    
    /**
     * Process an oplog entry for statistics
     */
    protected void processOplogEntry(RawBsonDocument doc, String shardId) {
        long docSize = doc.getByteBuffer().remaining();
        String ns = doc.getString("ns").getValue();
        String opType = doc.getString("op").getValue();
        
        // Track in accumulator
        OplogEntryKey key = new OplogEntryKey(ns, opType);
        EntryAccumulator acc = accumulators.get(key);
        if (acc == null) {
            acc = new EntryAccumulator(key);
            accumulators.put(key, acc);
        }
        acc.addExecution(docSize);
        
        // Track ID statistics if enabled
        if (idStats && idStatisticsManager != null) {
            BsonValue id = extractId(doc, opType);
            if (id != null) {
                if ("u".equals(opType) && fetchDocSizes) {
                    // For updates with fetchDocSizes, we need to fetch the actual document size
                    // This would be handled by the specific command implementation
                    idStatisticsManager.addIdStatisticsOplogOnly(ns, id, docSize);
                } else if ("i".equals(opType)) {
                    // For inserts, oplog contains the full document
                    idStatisticsManager.addIdStatistics(ns, id, docSize, docSize);
                } else {
                    // For deletes and other ops
                    idStatisticsManager.addIdStatisticsOplogOnly(ns, id, docSize);
                }
            }
        }
        
        // Log threshold exceedances
        if (docSize >= threshold) {
            logThresholdExceeded(shardId, ns, opType, docSize, doc);
        }
    }
    
    /**
     * Extract the _id from an oplog entry based on operation type
     */
    protected BsonValue extractId(RawBsonDocument doc, String opType) {
        try {
            if ("u".equals(opType)) {
                // For updates, _id is in o2 field
                BsonDocument o2 = doc.getDocument("o2");
                return o2 != null ? o2.get("_id") : null;
            } else {
                // For inserts and deletes, _id is in o field
                BsonDocument o = doc.getDocument("o");
                return o != null ? o.get("_id") : null;
            }
        } catch (Exception e) {
            // If we can't extract the ID, just return null
            return null;
        }
    }
    
    /**
     * Log when an operation exceeds the threshold
     */
    protected void logThresholdExceeded(String shardId, String ns, String opType, long docSize, RawBsonDocument doc) {
        // Extract the _id for better debugging
        BsonValue id = extractId(doc, opType);
        String idString = id != null ? getIdString(id) : "unknown";
        String sizeDisplay = org.apache.commons.io.FileUtils.byteCountToDisplaySize(docSize);
        
        if (shardId != null && !shardId.isEmpty()) {
            System.out.println(String.format("[%s] %s doc exceeded threshold (%s): {_id: %s }",
                shardId, ns, sizeDisplay, idString));
        } else {
            System.out.println(String.format("%s doc exceeded threshold (%s): {_id: %s }",
                ns, sizeDisplay, idString));
        }
    }
    
    /**
     * Format an ID value for display using the same logic as IdStatisticsManager
     */
    protected String getIdString(BsonValue id) {
        if (id == null) {
            return "null";
        }
        
        // Special handling for different BSON types
        switch (id.getBsonType()) {
            case BINARY:
                BsonBinary binary = id.asBinary();
                byte subtype = binary.getType();
                
                // Handle UUID types specially
                if (subtype == BsonBinarySubType.UUID_STANDARD.getValue() || 
                    subtype == BsonBinarySubType.UUID_LEGACY.getValue()) {
                    try {
                        UUID uuid = BsonUuidUtil.convertBsonBinaryToUuid(binary);
                        return uuid.toString();
                    } catch (Exception e) {
                        // Fall back to hex if UUID conversion fails
                    }
                }
                
                // For other binary types, show as compact hex
                byte[] data = binary.getData();
                if (data.length <= 16) {
                    // Short binary - show full hex
                    return toHexString(data);
                } else {
                    // Long binary - show truncated hex with length indicator
                    return toHexString(data, 8) + "..." + String.format("(%d bytes)", data.length);
                }
                
            case OBJECT_ID:
                // ObjectId already has a clean toString
                return id.asObjectId().getValue().toHexString();
                
            case STRING:
                return id.asString().getValue();
                
            case INT32:
                return String.valueOf(id.asInt32().getValue());
                
            case INT64:
                return String.valueOf(id.asInt64().getValue());
                
            case DOUBLE:
                return String.valueOf(id.asDouble().getValue());
                
            case BOOLEAN:
                return String.valueOf(id.asBoolean().getValue());
                
            default:
                // Fall back to BsonValueConverter for other types
                Object converted = BsonValueConverter.convertBsonValueToObject(id);
                return converted != null ? converted.toString() : id.toString();
        }
    }
    
    /**
     * Convert byte array to hex string
     */
    private String toHexString(byte[] bytes) {
        return toHexString(bytes, bytes.length);
    }
    
    /**
     * Convert byte array to hex string with specified length
     */
    private String toHexString(byte[] bytes, int maxBytes) {
        StringBuilder hex = new StringBuilder();
        int limit = Math.min(bytes.length, maxBytes);
        for (int i = 0; i < limit; i++) {
            hex.append(String.format("%02x", bytes[i] & 0xFF));
        }
        return hex.toString();
    }
    
    /**
     * Generate the final report
     */
    protected void report() {
        // Print accumulator statistics
        System.out.println();
        System.out.println("========== OPLOG ANALYSIS REPORT ==========");
        System.out.println(String.format("%-80s %5s %15s %15s %15s %15s %30s", "Namespace", "op", "count", "min", "max",
                "avg", "total (size)"));
        System.out.println(String.format("%-80s %5s %15s %15s %15s %15s %30s", 
            "=".repeat(80), "=".repeat(5), "=".repeat(15), "=".repeat(15), "=".repeat(15), "=".repeat(15), "=".repeat(30)));
        
        accumulators.values().stream()
            .sorted((e1, e2) -> Long.compare(e2.getTotal(), e1.getTotal()))
            .forEach(acc -> System.out.println(acc));
        
        // Print ID statistics if enabled
        if (idStats && idStatisticsManager != null && idStatisticsManager.hasStatistics()) {
            idStatisticsManager.printIdStatistics(fetchDocSizes);
        }
    }
    
    /**
     * Setup shutdown hook for graceful termination
     */
    protected void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("**** SHUTDOWN *****");
            stop();
        }));
    }
    
    protected void stop() {
        stop = true;
    }
    
    /**
     * Validate that either URI or file is provided
     */
    protected void validateInput() throws IllegalArgumentException {
        if (uri == null && inputFile == null) {
            throw new IllegalArgumentException("Either --uri or --file must be specified");
        }
        if (uri != null && inputFile != null) {
            throw new IllegalArgumentException("Cannot specify both --uri and --file");
        }
    }
    
    /**
     * Process a BSON file (regular or gzipped)
     */
    protected void processBsonFile() throws IOException {
        File file = new File(inputFile);
        if (!file.exists()) {
            throw new IOException("File not found: " + inputFile);
        }
        
        System.out.println("Processing BSON file: " + inputFile);
        long fileSize = file.length();
        System.out.println("File size: " + org.apache.commons.io.FileUtils.byteCountToDisplaySize(fileSize));
        
        try (InputStream inputStream = createInputStream(file)) {
            processBsonStream(inputStream, file.getName());
        }
    }
    
    /**
     * Create appropriate input stream based on file extension
     */
    private InputStream createInputStream(File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(fis, 65536); // 64KB buffer
        
        if (file.getName().endsWith(".gz")) {
            System.out.println("Detected gzipped file, decompressing on the fly...");
            return new GZIPInputStream(bis, 65536); // 64KB buffer for decompression
        } else {
            return bis;
        }
    }
    
    /**
     * Process BSON documents from an input stream
     */
    protected void processBsonStream(InputStream inputStream, String sourceName) throws IOException {
        byte[] sizeBytes = new byte[4];
        long count = 0;
        long totalBytes = 0;
        
        while (true) {
            // Read document size (first 4 bytes)
            int bytesRead = readFully(inputStream, sizeBytes);
            if (bytesRead < 4) {
                break; // End of file
            }
            
            // Parse document size (little-endian)
            ByteBuffer sizeBuf = ByteBuffer.wrap(sizeBytes).order(ByteOrder.LITTLE_ENDIAN);
            int docSize = sizeBuf.getInt();
            
            if (docSize < 5 || docSize > 16 * 1024 * 1024) { // Basic sanity check (5 bytes min, 16MB max)
                logger.warn("Invalid document size: {} at position {}", docSize, totalBytes);
                break;
            }
            
            // Read the rest of the document
            byte[] docBytes = new byte[docSize];
            System.arraycopy(sizeBytes, 0, docBytes, 0, 4);
            
            bytesRead = readFully(inputStream, docBytes, 4, docSize - 4);
            if (bytesRead < docSize - 4) {
                logger.warn("Incomplete document at position {}", totalBytes);
                break;
            }
            
            // Parse and process the document
            try {
                RawBsonDocument doc = new RawBsonDocument(docBytes);
                processOplogEntry(doc, sourceName);
                count++;
                totalBytes += docSize;
                
                if (count % 10000 == 0) {
                    System.out.println(String.format("Processed %,d entries (%s)", 
                        count, org.apache.commons.io.FileUtils.byteCountToDisplaySize(totalBytes)));
                }
            } catch (Exception e) {
                logger.warn("Error processing document at position {}: {}", totalBytes, e.getMessage());
            }
            
            if (stop) {
                break;
            }
        }
        
        System.out.println(String.format("Total processed from %s: %,d entries (%s)", 
            sourceName, count, org.apache.commons.io.FileUtils.byteCountToDisplaySize(totalBytes)));
    }
    
    /**
     * Read fully from input stream into buffer
     */
    private int readFully(InputStream input, byte[] buffer) throws IOException {
        return readFully(input, buffer, 0, buffer.length);
    }
    
    /**
     * Read fully from input stream into buffer at specific offset
     */
    private int readFully(InputStream input, byte[] buffer, int offset, int length) throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            int read = input.read(buffer, offset + totalRead, length - totalRead);
            if (read == -1) {
                break;
            }
            totalRead += read;
        }
        return totalRead;
    }
}