package com.mongodb.oploganalyzer;

import static com.mongodb.client.model.Filters.gte;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.shardsync.ShardClient;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command to collect representative samples of oplog entries per shard/namespace
 * with compressed BSON output for efficient analysis of workload patterns
 */
@Command(name = "sample", description = "Collect representative oplog samples per shard/namespace with compressed output")
public class SampleCommand extends BaseOplogCommand {
    
    private static final Logger logger = LoggerFactory.getLogger(SampleCommand.class);
    
    @Option(names = {"--idSampleSize"}, description = "Number of _id values to sample per shard/namespace (default: 100)", defaultValue = "100")
    private int idSampleSize = 100;
    
    @Option(names = {"--compress"}, description = "Compress output BSON file with gzip (default: true)", defaultValue = "true")
    private boolean compress = true;
    
    @Option(names = {"--statusInterval"}, description = "Status report interval in seconds (default: 30)", defaultValue = "30")
    private int statusInterval = 30;
    
    private ShardClient shardClient;
    private boolean shutdown = false;
    
    // Per-shard per-namespace ID sampling cache
    private final Map<String, Map<String, Set<String>>> shardNamespaceIdCache = new ConcurrentHashMap<>();
    
    // Output file handling - per shard
    private final Map<String, FileChannel> shardChannels = new ConcurrentHashMap<>();
    private final Map<String, GZIPOutputStream> shardGzipStreams = new ConcurrentHashMap<>();
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
    private String baseFileName;
    
    // Statistics
    private final AtomicLong totalEntriesProcessed = new AtomicLong(0);
    private final AtomicLong totalEntriesSampled = new AtomicLong(0);
    private final Map<String, AtomicLong> perShardCounts = new ConcurrentHashMap<>();
    
    // Status monitoring
    private ScheduledExecutorService statusMonitor;
    
    @Override
    public Integer call() {
        try {
            initializeClient();
            setupShutdownHook();
            startStatusMonitoring();
            sample();
            return 0;
        } catch (IOException e) {
            logger.error("Error sampling oplog", e);
            return 1;
        } finally {
            cleanup();
        }
    }
    
    private void initializeClient() {
        if (shardClient == null) {
            shardClient = new ShardClient("oplog-analyzer-sample", uri);
            shardClient.init();
            shardClient.populateShardMongoClients();
        }
    }
    
    protected void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n**** SAMPLE SHUTDOWN *****");
            shutdown = true;
            cleanup();
            printFinalReport();
        }));
    }
    
    private void startStatusMonitoring() {
        statusMonitor = Executors.newScheduledThreadPool(1);
        statusMonitor.scheduleAtFixedRate(() -> {
            printStatusUpdate();
        }, statusInterval, statusInterval, TimeUnit.SECONDS);
    }
    
    private void sample() throws IOException {
        initOutputFile();
        
        if (shardClient.isMongos()) {
            System.out.println("Sampling sharded cluster - collecting from all shards");
            // First collect shard key metadata
            collectShardKeyMetadata();
            sampleShardedCluster();
        } else {
            System.out.println("Sampling replica set - collecting from single oplog");
            sampleReplicaSet();
        }
    }
    
    private void initOutputFile() throws IOException {
        baseFileName = String.format("oplog_sample_%s", dateFormat.format(new java.util.Date()));
        System.out.println("Sampling to files: " + baseFileName + "_<shard>.bson" + (compress ? ".gz" : ""));
    }
    
    private void sampleReplicaSet() throws IOException {
        MongoClient mongoClient = shardClient.getMongoClient();
        sampleSingleShard("replica-set", mongoClient);
    }
    
    private void sampleShardedCluster() throws IOException {
        Map<String, MongoClient> shardClients = shardClient.getShardMongoClients();
        ExecutorService executor = Executors.newFixedThreadPool(shardClients.size());
        
        System.out.println(String.format("Starting sampling on %d shards", shardClients.size()));
        
        for (Map.Entry<String, MongoClient> entry : shardClients.entrySet()) {
            String shardId = entry.getKey();
            MongoClient mongoClient = entry.getValue();
            perShardCounts.put(shardId, new AtomicLong(0));
            
            executor.submit(() -> {
                try {
                    sampleSingleShard(shardId, mongoClient);
                } catch (Exception e) {
                    logger.error("Error sampling shard {}: {}", shardId, e.getMessage(), e);
                }
            });
        }
        
        // Wait for shutdown signal
        try {
            while (!shutdown) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            logger.debug("Main thread interrupted");
        }
        
        executor.shutdownNow();
    }
    
    private void sampleSingleShard(String shardId, MongoClient mongoClient) throws IOException {
        MongoCollection<RawBsonDocument> oplog = mongoClient.getDatabase("local")
            .getCollection("oplog.rs", RawBsonDocument.class);
        
        BsonTimestamp startTime = getLatestOplogTimestamp(mongoClient);
        logger.debug("[{}] Starting sample from timestamp: {}", shardId, startTime);
        
        MongoCursor<RawBsonDocument> cursor = null;
        
        try {
            cursor = oplog.find(gte("ts", startTime))
                .cursorType(CursorType.TailableAwait)
                .iterator();
            
            long processed = 0;
            
            while (cursor.hasNext() && !shutdown) {
                try {
                    RawBsonDocument doc = cursor.next();
                    processed++;
                    totalEntriesProcessed.incrementAndGet();
                    perShardCounts.get(shardId).incrementAndGet();
                    
                    String ns = ((BsonString) doc.get("ns")).getValue();
                    
                    if (ns.startsWith("config.")) {
                        continue;
                    }
                    
                    // Extract _id for sampling decision
                    BsonValue id = extractId(doc);
                    if (id == null) {
                        continue;
                    }
                    
                    String idString = getIdString(id);
                    
                    // Check if we should sample this entry
                    if (shouldSample(shardId, ns, idString)) {
                        writeSample(shardId, doc);
                        totalEntriesSampled.incrementAndGet();
                    }
                    
                } catch (MongoInterruptedException e) {
                    logger.debug("[{}] Sampling interrupted", shardId);
                    break;
                }
            }
            
            logger.info("[{}] Sampling completed. Processed {} entries", shardId, processed);
            
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }
    
    private boolean shouldSample(String shardId, String namespace, String idString) {
        Map<String, Set<String>> namespaceCache = shardNamespaceIdCache.computeIfAbsent(
            shardId, k -> new ConcurrentHashMap<>());
        
        Set<String> idSet = namespaceCache.computeIfAbsent(
            namespace, k -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
        
        // Always sample if we haven't reached the limit
        if (idSet.size() < idSampleSize) {
            idSet.add(idString);
            return true;
        }
        
        // Sample if we've seen this ID before (follow existing documents)
        return idSet.contains(idString);
    }
    
    private BsonValue extractId(RawBsonDocument doc) {
        try {
            String opType = ((BsonString) doc.get("op")).getValue();
            
            if ("u".equals(opType)) {
                // For updates, _id is in o2 field
                BsonDocument o2 = (BsonDocument) doc.get("o2");
                return o2 != null ? o2.get("_id") : null;
            } else {
                // For inserts and deletes, _id is in o field
                BsonDocument o = doc.getDocument("o");
                return o != null ? o.get("_id") : null;
            }
        } catch (Exception e) {
            return null;
        }
    }
    
    protected String getIdString(BsonValue id) {
        if (id == null) {
            return "null";
        }
        
        // Simplified ID string representation for sampling
        switch (id.getBsonType()) {
            case OBJECT_ID:
                return id.asObjectId().getValue().toHexString();
            case STRING:
                return id.asString().getValue();
            case BINARY:
                // For UUIDs and other binary, use first 8 bytes as hex
                try {
                    byte[] data = id.asBinary().getData();
                    return bytesToHex(data, Math.min(8, data.length));
                } catch (Exception e) {
                    return "binary";
                }
            case INT32:
                return String.valueOf(id.asInt32().getValue());
            case INT64:
                return String.valueOf(id.asInt64().getValue());
            default:
                // Truncate other types to reasonable length
                String str = id.toString();
                return str.length() > 32 ? str.substring(0, 32) : str;
        }
    }
    
    private String bytesToHex(byte[] bytes, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(String.format("%02x", bytes[i] & 0xff));
        }
        return sb.toString();
    }
    
    private synchronized void writeSample(String shardId, RawBsonDocument doc) {
        try {
            if (compress) {
                GZIPOutputStream gzipStream = shardGzipStreams.computeIfAbsent(shardId, k -> {
                    try {
                        String fileName = String.format("%s_%s.bson.gz", baseFileName, k);
                        FileOutputStream fos = new FileOutputStream(fileName);
                        return new GZIPOutputStream(fos);
                    } catch (IOException e) {
                        logger.error("Error creating gzip stream for shard {}", k, e);
                        return null;
                    }
                });
                
                if (gzipStream != null) {
                    ByteBuffer buffer = doc.getByteBuffer().asNIO();
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    gzipStream.write(bytes);
                    gzipStream.flush();
                }
            } else {
                FileChannel channel = shardChannels.computeIfAbsent(shardId, k -> {
                    try {
                        String fileName = String.format("%s_%s.bson", baseFileName, k);
                        FileOutputStream fos = new FileOutputStream(fileName);
                        return fos.getChannel();
                    } catch (IOException e) {
                        logger.error("Error creating channel for shard {}", k, e);
                        return null;
                    }
                });
                
                if (channel != null) {
                    ByteBuffer buffer = doc.getByteBuffer().asNIO();
                    channel.write(buffer);
                }
            }
        } catch (Exception e) {
            logger.error("Error writing sample for shard {}", shardId, e);
        }
    }
    
    private BsonTimestamp getLatestOplogTimestamp(MongoClient mongoClient) {
        MongoCollection<org.bson.Document> coll = mongoClient.getDatabase("local").getCollection("oplog.rs");
        org.bson.Document doc = coll.find()
            .comment("getSampleStartTimestamp")
            .projection(new org.bson.Document("ts", 1))
            .sort(new org.bson.Document("$natural", -1))
            .first();
        return doc != null ? (BsonTimestamp) doc.get("ts") : null;
    }
    
    private void printStatusUpdate() {
        if (shutdown) return;
        
        long processed = totalEntriesProcessed.get();
        long sampled = totalEntriesSampled.get();
        double sampleRate = processed > 0 ? (sampled * 100.0) / processed : 0;
        
        System.out.printf("Sampling: %,d processed, %,d sampled (%.1f%%), %d shards, %d namespaces%n",
            processed, sampled, sampleRate, 
            shardNamespaceIdCache.size(),
            shardNamespaceIdCache.values().stream()
                .mapToInt(Map::size)
                .sum());
    }
    
    private void printFinalReport() {
        System.out.println("\n=== SAMPLING REPORT ===");
        System.out.printf("Total entries processed: %,d%n", totalEntriesProcessed.get());
        System.out.printf("Total entries sampled: %,d%n", totalEntriesSampled.get());
        System.out.printf("Sample rate: %.2f%%%n", 
            totalEntriesProcessed.get() > 0 ? 
                (totalEntriesSampled.get() * 100.0) / totalEntriesProcessed.get() : 0);
        
        System.out.println("\nPer-shard processing:");
        perShardCounts.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(entry -> System.out.printf("  %s: %,d entries%n", 
                entry.getKey(), entry.getValue().get()));
        
        System.out.println("\nNamespace sampling summary:");
        for (Map.Entry<String, Map<String, Set<String>>> shardEntry : shardNamespaceIdCache.entrySet()) {
            String shardId = shardEntry.getKey();
            for (Map.Entry<String, Set<String>> nsEntry : shardEntry.getValue().entrySet()) {
                String namespace = nsEntry.getKey();
                int idCount = nsEntry.getValue().size();
                System.out.printf("  %s -> %s: %d sampled IDs%n", shardId, namespace, idCount);
            }
        }
    }
    
    private void collectShardKeyMetadata() {
        try {
            MongoClient configClient = shardClient.getMongoClient();
            MongoCollection<RawBsonDocument> configCollections = configClient.getDatabase("config")
                .getCollection("collections", RawBsonDocument.class);
            
            System.out.println("Collecting shard key metadata from config.collections...");
            
            List<RawBsonDocument> collections = new ArrayList<>();
            configCollections.find().into(collections);
            
            System.out.printf("Found %d sharded collections%n", collections.size());
            
            // Write config.collections documents to the first shard file (or dedicated config file)
            for (RawBsonDocument doc : collections) {
                writeSample("config", doc);
                totalEntriesSampled.incrementAndGet();
            }
            
        } catch (Exception e) {
            logger.error("Error collecting shard key metadata: {}", e.getMessage());
            System.err.println("Warning: Could not collect shard key metadata - continuing with oplog sampling");
        }
    }
    
    private void cleanup() {
        if (statusMonitor != null) {
            statusMonitor.shutdown();
        }
        
        // Close all shard GZIP streams
        for (Map.Entry<String, GZIPOutputStream> entry : shardGzipStreams.entrySet()) {
            try {
                GZIPOutputStream gzipStream = entry.getValue();
                if (gzipStream != null) {
                    gzipStream.finish();
                    gzipStream.close();
                }
            } catch (IOException e) {
                logger.error("Error closing gzip stream for shard {}", entry.getKey(), e);
            }
        }
        
        // Close all shard channels
        for (Map.Entry<String, FileChannel> entry : shardChannels.entrySet()) {
            try {
                FileChannel channel = entry.getValue();
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException e) {
                logger.error("Error closing channel for shard {}", entry.getKey(), e);
            }
        }
    }
}