package com.mongodb.oploganalyzer;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.include;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.mongodb.CursorType;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.bson.BsonUuidUtil;
import com.mongodb.util.bson.BsonValueConverter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "tail", description = "Tail oplog entries in real-time")
public class TailCommand implements Callable<Integer> {
    
    protected static final Logger logger = LoggerFactory.getLogger(TailCommand.class);
    
    private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH.mm");
    
    @Option(names = {"-c", "--uri"}, description = "MongoDB connection string URI", required = true)
    private String uri;
    
    @Option(names = {"-t", "--threshold"}, description = "Log operations >= this size (bytes)")
    private Long threshold = Long.MAX_VALUE;
    
    @Option(names = {"-l", "--limit"}, description = "Only examine limit number of oplog entries")
    private Integer limit = null;
    
    @Option(names = {"-d", "--dump"}, description = "Dump BSON to output file (mongodump format)")
    private boolean dump = false;
    
    @Option(names = {"--idStats"}, description = "Enable _id statistics tracking (use --idStatsThreshold to control which documents are tracked)")
    private boolean idStats = false;
    
    @Option(names = {"--idStatsThreshold"}, description = "Minimum size (bytes) for documents to be included in ID statistics (default: 0 = track all documents). Only effective with --idStats")
    private Long idStatsThreshold = 0L;
    
    @Option(names = {"--fetchDocSizes"}, description = "Fetch actual document sizes for updates (slower but more accurate). Only effective with --idStats")
    private boolean fetchDocSizes = false;
    
    @Option(names = {"--topIdCount"}, description = "Number of top frequent _id values to report (default 20)", defaultValue = "20")
    private int topIdCount = 20;
    
    @Option(names = {"--shardIndex"}, description = "Comma-separated list of shard indices to analyze (0,1,2...), default: all shards")
    private String shardIndexes;
    
    @Option(names = {"--shardStats"}, description = "Show per-shard breakdown of statistics in the report")
    private boolean shardStats = false;
    
    private ShardClient shardClient;
    private boolean shutdown = false;
    private Map<OplogEntryKey, EntryAccumulator> accumulators = new ConcurrentHashMap<OplogEntryKey, EntryAccumulator>();

    FileChannel channel;
    private Cache<String, IdStatistics> idStatsCache;
    
    // For tracking multi-threaded execution using worker pattern
    private ExecutorService currentExecutor = null;
    private List<ShardTailWorker> workers = new java.util.ArrayList<>();
    private List<Map<OplogEntryKey, EntryAccumulator>> shardAccumulators = null;
    private volatile boolean resultsAlreadyMerged = false;
    
    // Per-shard statistics (when shardStats flag is enabled)
    private Map<String, Map<OplogEntryKey, EntryAccumulator>> perShardStats = null;
    
    // Memory monitoring
    private ScheduledExecutorService memoryMonitor = null;
    private volatile long lastMemoryWarning = 0;
    private static final long MEMORY_WARNING_INTERVAL = 30000; // 30 seconds between warnings
    
    // Global statistics for memory tracking
    private volatile long totalBytesProcessed = 0;
    private volatile long totalDocsProcessed = 0;
    private volatile long largestDocSize = 0;
    private volatile String largestDocNamespace = "";
    
    // Shutdown handling
    private volatile int shutdownAttempts = 0;
    private volatile long lastShutdownAttempt = 0;
    private static final long SHUTDOWN_RESET_INTERVAL = 5000; // Reset counter after 5 seconds
    private volatile boolean reportGenerated = false;
    
    static class IdStatistics {
        long count = 0;
        // Document sizes
        long docSizeCount = 0; // Track how many valid doc sizes we have
        long minDocSize = Long.MAX_VALUE;
        long maxDocSize = Long.MIN_VALUE;
        long totalDocSize = 0;
        // Oplog entry sizes
        long minOplogSize = Long.MAX_VALUE;
        long maxOplogSize = Long.MIN_VALUE;
        long totalOplogSize = 0;
        
        void addSizes(long docSize, long oplogSize) {
            count++;
            // Track document sizes (skip if unknown, marked as -1)
            if (docSize >= 0) {
                docSizeCount++;
                totalDocSize += docSize;
                if (docSize < minDocSize) minDocSize = docSize;
                if (docSize > maxDocSize) maxDocSize = docSize;
            }
            // Track oplog sizes
            totalOplogSize += oplogSize;
            if (oplogSize < minOplogSize) minOplogSize = oplogSize;
            if (oplogSize > maxOplogSize) maxOplogSize = oplogSize;
        }
        
        double getAverageDocSize() {
            return docSizeCount > 0 ? (double) totalDocSize / docSizeCount : 0;
        }
        
        double getAverageOplogSize() {
            return count > 0 ? (double) totalOplogSize / count : 0;
        }
    }
    
    // Helper class to track pending updates that need size fetching
    static class PendingUpdate {
        final String ns;
        final BsonValue id;
        final long oplogEntrySize;
        final RawBsonDocument doc;
        
        PendingUpdate(String ns, BsonValue id, long oplogEntrySize, RawBsonDocument doc) {
            this.ns = ns;
            this.id = id;
            this.oplogEntrySize = oplogEntrySize;
            this.doc = doc;
        }
    }
    
    class ShardTailWorker implements Runnable {
        private final String shardId;
        private final MongoClient mongoClient;
        private final Map<OplogEntryKey, EntryAccumulator> targetAccumulators;
        
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicBoolean complete = new AtomicBoolean();
        
        // Batch fetching for update operations
        private final List<PendingUpdate> pendingUpdates = new ArrayList<>();
        private long lastBatchTime = System.currentTimeMillis();
        private static final int MAX_BATCH_SIZE = 10;
        private static final long BATCH_TIMEOUT_MS = 100; // 100ms timeout
        
        public ShardTailWorker(String shardId, MongoClient mongoClient, Map<OplogEntryKey, EntryAccumulator> targetAccumulators) {
            this.shardId = shardId;
            this.mongoClient = mongoClient;
            this.targetAccumulators = targetAccumulators;
        }
        
        public void start() {
            running.set(true);
        }
        
        public void stop() {
            running.set(false);
            while (!complete.get()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        
        public int getPendingUpdatesCount() {
            return pendingUpdates.size();
        }
        
        private void trackPerShardStats(String ns, String opType, long docSize) {
            if (shardStats && perShardStats != null) {
                Map<OplogEntryKey, EntryAccumulator> shardMap = perShardStats.get(shardId);
                if (shardMap != null) {
                    OplogEntryKey key = new OplogEntryKey(ns, opType);
                    EntryAccumulator accum = shardMap.get(key);
                    if (accum == null) {
                        accum = new EntryAccumulator(key, thresholdBuckets);
                        shardMap.put(key, accum);
                    }
                    accum.addExecution(docSize);
                }
            }
        }
        
        @Override
        public void run() {
            try {
                tailShardOplog();
            } catch (Exception e) {
                logger.error("Error tailing shard {}: {}", shardId, e.getMessage(), e);
            } finally {
                // Process any remaining pending updates (only if fetchDocSizes is enabled)
                if (fetchDocSizes && !pendingUpdates.isEmpty()) {
                    processPendingUpdates();
                }
                complete.set(true);
            }
        }
        
        private void processPendingUpdates() {
            if (pendingUpdates.isEmpty()) {
                return;
            }
            
            // Group pending updates by namespace
            Map<String, List<PendingUpdate>> updatesByNs = new HashMap<>();
            for (PendingUpdate update : pendingUpdates) {
                updatesByNs.computeIfAbsent(update.ns, k -> new ArrayList<>()).add(update);
            }
            
            // Process each namespace
            for (Map.Entry<String, List<PendingUpdate>> entry : updatesByNs.entrySet()) {
                String ns = entry.getKey();
                List<PendingUpdate> updates = entry.getValue();
                
                // Parse namespace to get database and collection
                String[] parts = ns.split("\\.", 2);
                if (parts.length != 2) {
                    continue;
                }
                String dbName = parts[0];
                String collName = parts[1];
                
                // Get the collection
                MongoDatabase db = mongoClient.getDatabase(dbName);
                MongoCollection<RawBsonDocument> collection = db.getCollection(collName, RawBsonDocument.class);
                
                // Prepare list of IDs for batch query
                List<BsonValue> ids = new ArrayList<>();
                for (PendingUpdate update : updates) {
                    ids.add(update.id);
                }
                
                try {
                    // Fetch actual documents
                    Map<BsonValue, Long> actualSizes = new HashMap<>();
                    collection.find(in("_id", ids))
                        .forEach(doc -> {
                            BsonValue docId = doc.get("_id");
                            if (docId != null) {
                                long actualSize = doc.getByteBuffer().remaining();
                                actualSizes.put(docId, actualSize);
                            }
                        });
                    
                    // Process each update with actual size
                    for (PendingUpdate update : updates) {
                        Long actualSize = actualSizes.get(update.id);
                        long sizeToUse = (actualSize != null) ? actualSize : update.oplogEntrySize;
                        
                        // Process the update with actual size
                        processUpdateWithSize(update, sizeToUse);
                    }
                } catch (Exception e) {
                    logger.warn("[{}] Failed to fetch actual document sizes for {}: {}", 
                        shardId, ns, e.getMessage());
                    // Fall back to oplog entry size
                    for (PendingUpdate update : updates) {
                        processUpdateWithSize(update, update.oplogEntrySize);
                    }
                }
            }
            
            pendingUpdates.clear();
        }
        
        private void processUpdateWithSize(PendingUpdate update, long actualSize) {
            String opType = "u"; // updates
            
            // Track in accumulator
            OplogEntryKey key = new OplogEntryKey(update.ns, opType);
            EntryAccumulator accum = targetAccumulators.get(key);
            if (accum == null) {
                accum = new EntryAccumulator(key, thresholdBuckets);
                targetAccumulators.put(key, accum);
            }
            accum.addExecution(actualSize);
            
            // Check main threshold for debug reporting
            if (actualSize >= threshold) {
                logThresholdExceeded(shardId, update.ns, "u", actualSize, update.doc);
            }
            
            // Check ID statistics threshold separately
            if (idStats && actualSize >= idStatsThreshold) {
                String idKey = update.ns + "::" + getIdString(update.id);
                IdStatistics stats = idStatsCache.get(idKey, k -> new IdStatistics());
                stats.addSizes(actualSize, update.oplogEntrySize);
            }
            
            // Handle dump if needed
            if (dump) {
                writeDump(update.doc);
            }
        }
        
        private void processUpdateOplogOnly(String ns, String opType, BsonValue id, long oplogSize, RawBsonDocument doc) {
            // Track in accumulator using oplog size
            OplogEntryKey key = new OplogEntryKey(ns, opType);
            EntryAccumulator accum = targetAccumulators.get(key);
            if (accum == null) {
                accum = new EntryAccumulator(key, thresholdBuckets);
                targetAccumulators.put(key, accum);
            }
            accum.addExecution(oplogSize);
            
            // Track per-shard stats if enabled
            trackPerShardStats(ns, opType, oplogSize);
            
            // Check main threshold for debug reporting (using oplog size - not ideal but fast)
            if (oplogSize >= threshold) {
                logThresholdExceeded(shardId, ns, opType, oplogSize, doc);
            }
            
            // Check ID statistics threshold separately (using oplog size)
            if (idStats && oplogSize >= idStatsThreshold) {
                String idKey = ns + "::" + getIdString(id);
                IdStatistics stats = idStatsCache.get(idKey, k -> new IdStatistics());
                // Only oplog size known, document size unknown (-1)
                stats.addSizes(-1, oplogSize);
            }
            
            // Handle dump if needed
            if (dump) {
                writeDump(doc);
            }
        }
        
        private void tailShardOplog() {
            complete.set(false);
            
            MongoDatabase local = mongoClient.getDatabase("local");
            MongoCollection<RawBsonDocument> oplog = local.getCollection("oplog.rs", RawBsonDocument.class);

            BsonTimestamp shardTimestamp = getLatestOplogTimestamp(mongoClient);
            logger.debug("[{}] Latest oplog timestamp: {}", shardId, shardTimestamp);

            MongoCursor<RawBsonDocument> cursor = null;
            
            try {
                cursor = oplog.find(gte("ts", shardTimestamp)).cursorType(CursorType.TailableAwait).iterator();
                
                long count = 0;
                long lastReportTime = System.currentTimeMillis();
                BsonTimestamp lastOplogTimestamp = null;
                
                while (cursor.hasNext() && running.get()) {
                    try {
                        RawBsonDocument doc = cursor.next();
                        
                        // Track latest oplog timestamp for lag calculation
                        BsonTimestamp currentOplogTs = doc.getTimestamp("ts");
                        if (currentOplogTs != null) {
                            lastOplogTimestamp = currentOplogTs;
                        }
                        
                        String ns = ((BsonString) doc.get("ns")).getValue();
                        BsonString op = (BsonString) doc.get("op");
                        String opType = op.getValue();

                        if (ns.startsWith("config.")) {
                            continue;
                        }

                        long docSize = doc.getByteBuffer().remaining();
                        
                        // Track global statistics
                        synchronized (this) {
                            totalBytesProcessed += docSize;
                            totalDocsProcessed++;
                            if (docSize > largestDocSize) {
                                largestDocSize = docSize;
                                largestDocNamespace = ns;
                            }
                        }
                        
                        // Handle applyOps operations (transactions/bulk ops)
                        if ("c".equals(opType) && ns.endsWith(".$cmd")) {
                            BsonDocument o = (BsonDocument) doc.get("o");
                            if (o != null && o.containsKey("applyOps")) {
                                BsonArray applyOps = o.getArray("applyOps");
                                for (BsonValue applyOp : applyOps) {
                                    if (applyOp.isDocument()) {
                                        BsonDocument innerOp = applyOp.asDocument();
                                        String innerNs = innerOp.getString("ns", new BsonString("unknown")).getValue();
                                        String innerOpType = innerOp.getString("op", new BsonString("unknown")).getValue();
                                        
                                        if (!innerNs.startsWith("config.")) {
                                            OplogEntryKey innerKey = new OplogEntryKey(innerNs, innerOpType);
                                            EntryAccumulator innerAccum = targetAccumulators.get(innerKey);
                                            if (innerAccum == null) {
                                                innerAccum = new EntryAccumulator(innerKey, thresholdBuckets);
                                                targetAccumulators.put(innerKey, innerAccum);
                                            }
                                            // Use a portion of the total doc size for each nested op
                                            long innerDocSize = docSize / applyOps.size();
                                            innerAccum.addExecution(innerDocSize);
                                            
                                            // Track per-shard stats if enabled
                                            trackPerShardStats(innerNs, innerOpType, innerDocSize);
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Handle update operations
                        if ("u".equals(opType)) {
                            BsonDocument o2 = (BsonDocument) doc.get("o2");
                            if (o2 != null) {
                                BsonValue id = o2.get("_id");
                                if (id != null) {
                                    if (fetchDocSizes && idStats) {
                                        // Add to pending updates for batch processing (fetch actual doc sizes)
                                        pendingUpdates.add(new PendingUpdate(ns, id, docSize, doc));
                                        
                                        // Check if batch should be processed
                                        long currentTime = System.currentTimeMillis();
                                        if (pendingUpdates.size() >= MAX_BATCH_SIZE || 
                                            (currentTime - lastBatchTime) >= BATCH_TIMEOUT_MS) {
                                            processPendingUpdates();
                                            lastBatchTime = currentTime;
                                        }
                                    } else {
                                        // Process immediately using oplog entry size only
                                        processUpdateOplogOnly(ns, opType, id, docSize, doc);
                                    }
                                }
                            }
                        } else {
                            // For non-update operations (inserts, deletes), process immediately
                            OplogEntryKey key = new OplogEntryKey(ns, opType);
                            EntryAccumulator accum = targetAccumulators.get(key);
                            if (accum == null) {
                                accum = new EntryAccumulator(key, thresholdBuckets);
                                targetAccumulators.put(key, accum);
                            }
                            accum.addExecution(docSize);
                            
                            // Track per-shard stats if enabled
                            trackPerShardStats(ns, opType, docSize);
                            
                            // Get the _id for both debug and statistics purposes
                            BsonDocument o = (BsonDocument) doc.get("o");
                            BsonValue id = null;
                            if (o != null) {
                                id = o.get("_id");
                            }
                            
                            // Check main threshold for debug reporting
                            if (docSize >= threshold) {
                                if (id != null) {
                                    logThresholdExceeded(shardId, ns, opType, docSize, doc);
                                } else {
                                    System.out.println("doc exceeded threshold, but no _id in the 'o' field");
                                }
                            }
                            
                            // Check ID statistics threshold separately
                            if (idStats && docSize >= idStatsThreshold && id != null) {
                                String idKey = ns + "::" + getIdString(id);
                                IdStatistics stats = idStatsCache.get(idKey, k -> new IdStatistics());
                                // For inserts, oplog contains full doc; for deletes, just _id
                                // We can't get the actual doc size for deletes (doc is gone)
                                if ("i".equals(opType)) {
                                    // Insert: oplog size = document size
                                    stats.addSizes(docSize, docSize);
                                } else if ("d".equals(opType)) {
                                    // Delete: we don't know actual doc size, use -1 as marker
                                    stats.addSizes(-1, docSize);
                                } else {
                                    // Other ops: use oplog size for both (shouldn't happen here)
                                    stats.addSizes(docSize, docSize);
                                }
                            }
                            
                            if (dump) {
                                writeDump(doc);
                            }
                        }
                        
                        count++;
                        
                        // Report every 30 seconds
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastReportTime >= 30000) {
                            // Process any pending updates before reporting (only if fetchDocSizes is enabled)
                            if (fetchDocSizes && !pendingUpdates.isEmpty()) {
                                processPendingUpdates();
                                lastBatchTime = currentTime;
                            }
                            
                            long lagSeconds = calculateLagSeconds(lastOplogTimestamp);
                            logger.info("[{}] Processed {} entries, Lag: {}s, {}", 
                                shardId, String.format("%,d", count), lagSeconds, getMemoryStats());
                            lastReportTime = currentTime;
                        }
                        
                        if (limit != null && count >= limit) {
                            break;
                        }
                        
                    } catch (MongoInterruptedException e) {
                        logger.debug("[{}] Interrupted: {}", shardId, e.getMessage());
                        break;
                    }
                }
                
                // Process any remaining pending updates (only if fetchDocSizes is enabled)
                if (fetchDocSizes && !pendingUpdates.isEmpty()) {
                    processPendingUpdates();
                }
                
                // Final report with lag
                long finalLagSeconds = calculateLagSeconds(lastOplogTimestamp);
                logger.info("[{}] Total processed: {} entries, Final lag: {}s", 
                    shardId, String.format("%,d", count), finalLagSeconds);
                
            } catch (MongoInterruptedException e) {
                logger.debug("[{}] MongoDB interrupted: {}", shardId, e.getMessage());
            } catch (Exception e) {
                logger.error("[{}] Error during tailing: {}", shardId, e.getMessage(), e);
            } finally {
                if (cursor != null) {
                    try {
                        cursor.close();
                    } catch (Exception e) {
                        logger.debug("[{}] Error closing cursor: {}", shardId, e.getMessage());
                    }
                }
            }
        }
    }

    @Override
    public Integer call() {
        try {
            initializeClient();
            setupShutdownHook();
            startMemoryMonitoring();
            analyze();
            return 0;
        } catch (IOException e) {
            logger.error("Error analyzing oplog", e);
            return 1;
        } finally {
            stopMemoryMonitoring();
        }
    }
    
    private void setupShutdownHook() {
        if (limit == null) {
            // First, add a regular shutdown hook for the initial Ctrl-C
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                public void run() {
                    handleShutdown();
                }
            }));
            
            // Also register a signal handler to track multiple Ctrl-C attempts
            try {
                sun.misc.Signal.handle(new sun.misc.Signal("INT"), signal -> {
                    long now = System.currentTimeMillis();
                    
                    // Reset counter if it's been more than 5 seconds since last attempt
                    if (now - lastShutdownAttempt > SHUTDOWN_RESET_INTERVAL) {
                        shutdownAttempts = 0;
                    }
                    
                    shutdownAttempts++;
                    lastShutdownAttempt = now;
                    
                    if (shutdownAttempts == 1) {
                        System.out.println("\n>>> Graceful shutdown initiated... (press Ctrl-C again to force quit)");
                        // First attempt - graceful shutdown will be handled by shutdown hook
                    } else if (shutdownAttempts == 2) {
                        System.out.println("\n>>> Forcing shutdown... (press Ctrl-C once more for immediate termination)");
                        forceShutdown();
                    } else if (shutdownAttempts >= 3) {
                        System.err.println("\n>>> IMMEDIATE TERMINATION!");
                        Runtime.getRuntime().halt(1);
                    }
                });
            } catch (IllegalArgumentException e) {
                // Signal handling not supported on this platform
                logger.debug("Signal handling not available: {}", e.getMessage());
            }
        }
    }
    
    private void handleShutdown() {
        System.out.println();
        System.out.println("**** SHUTDOWN *****");
        stop();
        
        // If we have multi-threaded execution, stop workers and merge results
        if (workers != null && !workers.isEmpty()) {
            stopWorkers();
            if (!resultsAlreadyMerged) {
                mergeShardResults();
            }
        }
        
        // Only report if we haven't already reported
        if (!reportGenerated) {
            report();
            reportGenerated = true;
        }
    }
    
    private void forceShutdown() {
        // More aggressive shutdown - shorter timeouts
        Thread forcedShutdown = new Thread(() -> {
            try {
                System.out.println("Forcing worker termination...");
                
                // Give workers only 2 seconds to stop gracefully
                if (currentExecutor != null) {
                    currentExecutor.shutdownNow();
                    if (!currentExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                        System.err.println("Workers did not terminate in time!");
                    }
                }
                
                // Stop memory monitoring immediately
                if (memoryMonitor != null) {
                    memoryMonitor.shutdownNow();
                }
                
                // Try to generate a quick report with whatever data we have
                if (!resultsAlreadyMerged && shardAccumulators != null) {
                    mergeShardResults();
                }
                if (!reportGenerated) {
                    report();
                    reportGenerated = true;
                }
                
            } catch (Exception e) {
                System.err.println("Error during forced shutdown: " + e.getMessage());
            } finally {
                // Exit after maximum 3 seconds
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    // Ignore
                }
                System.exit(1);
            }
        });
        forcedShutdown.setDaemon(true);
        forcedShutdown.start();
    }
    
    private void initializeClient() {
        if (shardClient == null) {
            shardClient = new ShardClient("oplog-analyzer", uri);
            shardClient.init();
            shardClient.populateShardMongoClients();
            idStatsCache = Caffeine.newBuilder()
                .maximumSize(100000)
                .build();
        }
    }
    
    public void analyze() throws IOException {
        if (dump) {
            initDumpFile();
        }

        if (shardClient.isMongos()) {
            System.out.println("Detected sharded cluster - tailing all shards simultaneously");
            analyzeShardedCluster();
        } else {
            System.out.println("Detected replica set - tailing single oplog");
            analyzeReplicaSet();
        }
    }
    
    private void analyzeReplicaSet() throws IOException {
        MongoClient mongoClient = shardClient.getMongoClient();
        tailSingleShard("replica-set", mongoClient);
    }
    
    private void analyzeShardedCluster() throws IOException {
        Map<String, MongoClient> shardClients = shardClient.getShardMongoClients();
        List<String> targetShards = getTargetShards(shardClients);
        
        if (targetShards.size() == 1) {
            String shardId = targetShards.get(0);
            MongoClient mongoClient = shardClients.get(shardId);
            System.out.println(String.format("Tailing single shard: %s", shardId));
            tailSingleShard(shardId, mongoClient);
        } else {
            System.out.println(String.format("Tailing %d shards in parallel", targetShards.size()));
            tailMultipleShards(shardClients, targetShards);
        }
    }
    
    private List<String> getTargetShards(Map<String, MongoClient> shardClients) {
        if (shardIndexes == null) {
            return shardClients.keySet().stream().collect(Collectors.toList());
        }
        
        String[] indices = shardIndexes.split(",");
        List<String> shardIds = shardClients.keySet().stream().sorted().collect(Collectors.toList());
        List<String> targetShards = new java.util.ArrayList<>();
        
        for (String indexStr : indices) {
            try {
                int index = Integer.parseInt(indexStr.trim());
                if (index >= 0 && index < shardIds.size()) {
                    targetShards.add(shardIds.get(index));
                } else {
                    logger.warn("Shard index {} is out of range (0-{}), skipping", index, shardIds.size() - 1);
                }
            } catch (NumberFormatException e) {
                logger.warn("Invalid shard index '{}', skipping", indexStr);
            }
        }
        
        return targetShards;
    }
    
    private void tailMultipleShards(Map<String, MongoClient> shardClients, List<String> targetShards) {
        currentExecutor = Executors.newFixedThreadPool(targetShards.size());
        workers = new java.util.ArrayList<>();
        shardAccumulators = new java.util.ArrayList<>();
        
        // Initialize per-shard stats if requested
        if (shardStats) {
            perShardStats = new ConcurrentHashMap<>();
        }
        
        // Create and start workers for each shard
        for (String shardId : targetShards) {
            MongoClient mongoClient = shardClients.get(shardId);
            
            // Create separate accumulator for this shard - no contention!
            Map<OplogEntryKey, EntryAccumulator> shardAccumulator = new HashMap<>();
            shardAccumulators.add(shardAccumulator);
            
            // Initialize per-shard stats tracking
            if (shardStats) {
                perShardStats.put(shardId, new ConcurrentHashMap<>());
            }
            
            ShardTailWorker worker = new ShardTailWorker(shardId, mongoClient, shardAccumulator);
            workers.add(worker);
            currentExecutor.execute(worker);
            worker.start(); // Set running flag to true
        }
        
        try {
            // Wait indefinitely until shutdown
            while (!shutdown) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            logger.debug("Main thread interrupted");
        }
        
        // Gracefully stop all workers
        stopWorkers();
        
        // Merge results from separate shard accumulators
        mergeShardResults();
    }
    
    private void stopWorkers() {
        stopWorkers(5);  // Default 5 second timeout
    }
    
    private void stopWorkers(int timeoutSeconds) {
        logger.debug("Stopping {} workers with {}s timeout", workers.size(), timeoutSeconds);
        for (ShardTailWorker worker : workers) {
            worker.stop();
        }
        
        currentExecutor.shutdown();
        try {
            if (!currentExecutor.awaitTermination(timeoutSeconds, java.util.concurrent.TimeUnit.SECONDS)) {
                logger.warn("Workers did not terminate within {}s timeout, forcing shutdown", timeoutSeconds);
                currentExecutor.shutdownNow();
                // Give it one more second after shutdownNow
                if (!currentExecutor.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS)) {
                    logger.error("Workers still running after forced shutdown!");
                }
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for workers to stop");
            currentExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        logger.debug("All workers stopped");
    }
    
    private void tailSingleShard(String shardId, MongoClient mongoClient) throws IOException {
        // For single shard, create one worker and run it directly
        ShardTailWorker worker = new ShardTailWorker(shardId, mongoClient, accumulators);
        worker.start();
        worker.run(); // Run directly instead of submitting to executor
    }
    
    
    private long calculateLagSeconds(BsonTimestamp oplogTimestamp) {
        if (oplogTimestamp == null) {
            return -1; // Unknown lag
        }
        
        long currentWallClockSeconds = System.currentTimeMillis() / 1000;
        long oplogSeconds = oplogTimestamp.getTime();
        
        return Math.max(0, currentWallClockSeconds - oplogSeconds);
    }
    
    private long mergeResults(Map<OplogEntryKey, EntryAccumulator> shardResults) {
        long totalEntries = 0;
        for (Map.Entry<OplogEntryKey, EntryAccumulator> entry : shardResults.entrySet()) {
            OplogEntryKey key = entry.getKey();
            EntryAccumulator shardAcc = entry.getValue();
            totalEntries += shardAcc.getCount();
            
            EntryAccumulator globalAcc = accumulators.get(key);
            if (globalAcc == null) {
                globalAcc = new EntryAccumulator(key, thresholdBuckets);
                accumulators.put(key, globalAcc);
            }
            
            // Merge the accumulator data properly
            globalAcc.merge(shardAcc);
        }
        return totalEntries;
    }
    
    private void mergeShardResults() {
        // Prevent double execution (both normal completion and shutdown hook)
        if (resultsAlreadyMerged) {
            logger.debug("Results already merged, skipping");
            return;
        }
        
        synchronized (this) {
            if (resultsAlreadyMerged) {
                logger.debug("Results already merged (double-check), skipping");
                return;
            }
            resultsAlreadyMerged = true;
        }
        
        logger.debug("Starting merge of shard results");
        
        if (shardAccumulators == null) {
            logger.debug("No shard accumulators to merge");
            return;
        }
        
        logger.debug("Found {} shard accumulators to merge", shardAccumulators.size());
        
        // Merge results from separate per-thread accumulators - no contention!
        long totalCount = 0;
        for (int i = 0; i < shardAccumulators.size() && i < workers.size(); i++) {
            String shardId = workers.get(i).shardId;
            Map<OplogEntryKey, EntryAccumulator> shardResults = shardAccumulators.get(i);
            
            logger.debug("Checking shard {} accumulator with {} entries", shardId, 
                shardResults != null ? shardResults.size() : 0);
            
            if (shardResults != null && !shardResults.isEmpty()) {
                long shardCount = mergeResults(shardResults);
                totalCount += shardCount;
                logger.info("Merged {} operations from shard: {}", shardCount, shardId);
            } else {
                logger.debug("No results from shard: {}", shardId);
            }
        }
        
        logger.info("Total processed across all shards: {} entries", String.format("%,d", totalCount));
    }
    
    /**
     * Log when an operation exceeds the threshold (consistent with BaseOplogCommand)
     */
    private void logThresholdExceeded(String shardId, String ns, String opType, long docSize, RawBsonDocument doc) {
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
     * Extract the _id from an oplog entry based on operation type (consistent with BaseOplogCommand)
     */
    private BsonValue extractId(RawBsonDocument doc, String opType) {
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
    
    private String getIdString(BsonValue id) {
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
    
    private String formatIdKey(String idKey) {
        // Split the namespace::id format
        String[] parts = idKey.split("::", 2);
        if (parts.length != 2) {
            return idKey; // Return as-is if format is unexpected
        }
        
        String ns = parts[0];
        String id = parts[1];
        
        // No need for special BsonObjectId handling since getIdString() now uses BsonValueConverter
        // which returns clean string representations
        
        return ns + "::" + id;
    }
    
    private void initDumpFile() throws IOException {
        String fileName = String.format("oplog_%s.bson", dateFormat.format(new java.util.Date()));
        try (FileOutputStream outputStream = new FileOutputStream(fileName)) {
			channel = outputStream.getChannel();
		} catch (FileNotFoundException e) {
			throw e;
		}
        System.out.println("Dumping to file: " + fileName);
    }
    
    private void writeDump(RawBsonDocument doc) {
        try {
            byte[] bytes = doc.getByteBuffer().array();
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            channel.write(buffer);
        } catch (Exception e) {
            logger.error("Error writing dump", e);
        }
    }
    
    private BsonTimestamp getLatestOplogTimestamp(MongoClient mongoClient) {
        MongoCollection<Document> coll = mongoClient.getDatabase("local").getCollection("oplog.rs");
        Document doc = null;
        doc = coll.find().comment("getLatestOplogTimestamp").projection(include("ts")).sort(eq("$natural", -1)).first();
        BsonTimestamp ts = (BsonTimestamp) doc.get("ts");
        return ts;
    }
    
    private void startMemoryMonitoring() {
        memoryMonitor = Executors.newScheduledThreadPool(1);
        memoryMonitor.scheduleAtFixedRate(() -> {
            Runtime runtime = Runtime.getRuntime();
            long maxMemory = runtime.maxMemory();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            double usedPercent = (usedMemory * 100.0) / maxMemory;
            
            // Calculate data structure sizes
            int totalAccumulators = 0;
            int totalPendingUpdates = 0;
            long cacheSize = 0;
            
            // Count accumulators across all shards
            if (shardAccumulators != null) {
                for (Map<OplogEntryKey, EntryAccumulator> shardAccum : shardAccumulators) {
                    if (shardAccum != null) {
                        totalAccumulators += shardAccum.size();
                    }
                }
            }
            // Main accumulator map
            if (accumulators != null) {
                totalAccumulators += accumulators.size();
            }
            
            // Count pending updates across all workers
            if (workers != null) {
                for (ShardTailWorker worker : workers) {
                    totalPendingUpdates += worker.getPendingUpdatesCount();
                }
            }
            
            // Cache size
            if (idStatsCache != null) {
                cacheSize = idStatsCache.estimatedSize();
            }
            
            // Log memory stats every 5 seconds at DEBUG level with data structure info
            logger.debug("Memory: Used={} MB ({}%), Free={} MB, Max={} MB | Accumulators={}, Cache={}, PendingUpdates={} | Docs={}, TotalBytes={} MB", 
                usedMemory / (1024 * 1024),
                String.format("%.1f", usedPercent),
                freeMemory / (1024 * 1024),
                maxMemory / (1024 * 1024),
                totalAccumulators,
                cacheSize,
                totalPendingUpdates,
                totalDocsProcessed,
                totalBytesProcessed / (1024 * 1024));
            
            // Warn if memory usage is high (>80%)
            if (usedPercent > 80) {
                long now = System.currentTimeMillis();
                if (now - lastMemoryWarning > MEMORY_WARNING_INTERVAL) {
                    logger.warn("High memory usage: {}% ({} MB / {} MB). Consider increasing heap size with -Xmx", 
                        String.format("%.1f", usedPercent),
                        usedMemory / (1024 * 1024),
                        maxMemory / (1024 * 1024));
                    logger.warn("Data structures: Accumulators={}, IdStatsCache={}, PendingUpdates={}", 
                        totalAccumulators, cacheSize, totalPendingUpdates);
                    logger.warn("Processing stats: Docs={}, TotalBytes={} MB, LargestDoc={} KB in {}", 
                        totalDocsProcessed, 
                        totalBytesProcessed / (1024 * 1024),
                        largestDocSize / 1024,
                        largestDocNamespace);
                    
                    // Log top memory consumers if we have them
                    if (totalAccumulators > 1000) {
                        logger.warn("Large number of accumulators detected ({} entries). Consider using namespace filtering.", totalAccumulators);
                    }
                    if (cacheSize > 10000) {
                        logger.warn("Large ID cache size ({} entries). Consider reducing --topIdCount or disabling --idStats.", cacheSize);
                    }
                    
                    // Check if we're holding too many raw BSON documents
                    long avgDocSize = totalDocsProcessed > 0 ? totalBytesProcessed / totalDocsProcessed : 0;
                    if (avgDocSize > 100 * 1024) { // avg > 100KB
                        logger.warn("Large average document size: {} KB. Consider filtering large namespaces.", avgDocSize / 1024);
                    }
                    lastMemoryWarning = now;
                }
            }
            
            // Critical memory usage (>95%) - log top namespaces and trigger GC
            if (usedPercent > 95) {
                logger.error("CRITICAL memory usage: {}%", String.format("%.1f", usedPercent));
                
                if (accumulators != null && !accumulators.isEmpty()) {
                    logger.error("Top 5 namespaces by operation count:");
                    accumulators.values().stream()
                        .sorted(Comparator.comparingLong(EntryAccumulator::getCount).reversed())
                        .limit(5)
                        .forEach(acc -> logger.error("  {}: {} ops, total size: {} KB", 
                            acc.getNamespace(), 
                            acc.getCount(),
                            acc.getTotal() / 1024));
                }
                
                // Try to free memory by forcing GC
                logger.warn("Attempting garbage collection due to critical memory usage...");
                System.gc();
                
                // Re-check memory after GC
                try {
                    Thread.sleep(500); // Give GC time to run
                } catch (InterruptedException e) {
                    // Ignore
                }
                
                Runtime postGcRuntime = Runtime.getRuntime();
                long postGcUsed = postGcRuntime.totalMemory() - postGcRuntime.freeMemory();
                double postGcPercent = (postGcUsed * 100.0) / postGcRuntime.maxMemory();
                logger.info("Memory after GC: {}% ({} MB / {} MB)", 
                    String.format("%.1f", postGcPercent),
                    postGcUsed / (1024 * 1024),
                    postGcRuntime.maxMemory() / (1024 * 1024));
            }
        }, 0, 5, TimeUnit.SECONDS);
    }
    
    private void stopMemoryMonitoring() {
        if (memoryMonitor != null) {
            memoryMonitor.shutdown();
            try {
                if (!memoryMonitor.awaitTermination(5, TimeUnit.SECONDS)) {
                    memoryMonitor.shutdownNow();
                }
            } catch (InterruptedException e) {
                memoryMonitor.shutdownNow();
            }
        }
    }
    
    private String getMemoryStats() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        double usedPercent = (usedMemory * 100.0) / maxMemory;
        
        return String.format("Mem: %.1f%% (%dMB/%dMB)", 
            usedPercent, 
            usedMemory / (1024 * 1024), 
            maxMemory / (1024 * 1024));
    }
    
    public void report() {
        System.out.println();
        System.out.println(String.format("%-80s %5s %15s %15s %15s %15s %30s", "Namespace", "op", "count", "min", "max",
                "avg", "total (size)"));
        accumulators.values().stream().sorted(Comparator.comparingLong(EntryAccumulator::getCount).reversed())
                .forEach(acc -> System.out.println(acc));
        
        if (idStats && !idStatsCache.asMap().isEmpty()) {
            printIdStatistics();
        }
        
        if (shardStats && perShardStats != null && !perShardStats.isEmpty()) {
            printPerShardStatistics();
        }
    }
    
    private void printPerShardStatistics() {
        System.out.println();
        System.out.println("=== PER-SHARD BREAKDOWN ===");
        
        // Sort shards by name for consistent output
        List<String> sortedShards = perShardStats.keySet().stream().sorted().collect(Collectors.toList());
        
        for (String shardId : sortedShards) {
            Map<OplogEntryKey, EntryAccumulator> shardAccumulatorMap = perShardStats.get(shardId);
            if (shardAccumulatorMap != null && !shardAccumulatorMap.isEmpty()) {
                System.out.println();
                System.out.println(String.format("--- %s ---", shardId));
                System.out.println(String.format("%-80s %5s %15s %15s %15s %15s %30s", "Namespace", "op", "count", "min", "max",
                        "avg", "total (size)"));
                
                // Sort by operation count descending
                shardAccumulatorMap.values().stream()
                    .sorted(Comparator.comparingLong(EntryAccumulator::getCount).reversed())
                    .forEach(acc -> System.out.println(acc));
                
                // Summary for this shard
                long totalOps = shardAccumulatorMap.values().stream().mapToLong(EntryAccumulator::getCount).sum();
                long totalBytes = shardAccumulatorMap.values().stream().mapToLong(EntryAccumulator::getTotal).sum();
                System.out.println(String.format("Shard %s total: %,d operations, %s", 
                    shardId, totalOps, org.apache.commons.io.FileUtils.byteCountToDisplaySize(totalBytes)));
            }
        }
    }
    
    private void printIdStatistics() {
        System.out.println();
        System.out.println("Top " + topIdCount + " most frequent _id values:");
        
        Map<String, IdStatistics> statsMap = idStatsCache.asMap();
        List<Map.Entry<String, IdStatistics>> sortedEntries = statsMap.entrySet().stream()
            .sorted((e1, e2) -> Long.compare(e2.getValue().count, e1.getValue().count))
            .limit(topIdCount)
            .collect(Collectors.toList());
        
        // Calculate dynamic column widths based on actual content
        int maxIdKeyLength = "Namespace::_id".length();
        for (Map.Entry<String, IdStatistics> entry : sortedEntries) {
            String formattedIdKey = formatIdKey(entry.getKey());
            maxIdKeyLength = Math.max(maxIdKeyLength, formattedIdKey.length());
        }
        
        // Set reasonable bounds for the ID column width
        maxIdKeyLength = Math.max(30, Math.min(maxIdKeyLength + 2, 80));
        
        if (fetchDocSizes) {
            // Show both document and oplog sizes when fetchDocSizes is enabled
            String headerFormat = "%-" + maxIdKeyLength + "s %8s %15s %15s %15s";
            System.out.println(String.format(headerFormat, 
                "Namespace::_id", "Count", "Avg Doc Size", "Avg Oplog Size", "Doc/Oplog Ratio"));
            System.out.println("=".repeat(maxIdKeyLength + 8 + 15 + 15 + 15 + 4));
        } else {
            // Show only oplog-based information when fetchDocSizes is disabled
            String headerFormat = "%-" + maxIdKeyLength + "s %8s %15s";
            System.out.println(String.format(headerFormat, 
                "Namespace::_id", "Count", "Avg Oplog Size"));
            System.out.println("=".repeat(maxIdKeyLength + 8 + 15 + 2));
        }
            
        for (Map.Entry<String, IdStatistics> entry : sortedEntries) {
            String formattedIdKey = formatIdKey(entry.getKey());
            IdStatistics stats = entry.getValue();
            String avgOplogSizeStr = org.apache.commons.io.FileUtils.byteCountToDisplaySize((long)stats.getAverageOplogSize());
            
            if (fetchDocSizes) {
                // Show full information when document sizes were fetched
                String avgDocSizeStr = stats.docSizeCount > 0 ? 
                    org.apache.commons.io.FileUtils.byteCountToDisplaySize((long)stats.getAverageDocSize()) : "N/A";
                
                // Calculate ratio of doc size to oplog size (useful for understanding update efficiency)
                String ratioStr = "N/A";
                if (stats.docSizeCount > 0 && stats.getAverageOplogSize() > 0) {
                    double ratio = stats.getAverageDocSize() / stats.getAverageOplogSize();
                    if (ratio < 10) {
                        ratioStr = String.format("%.1fx", ratio);
                    } else {
                        ratioStr = String.format("%.0fx", ratio);
                    }
                }
                
                String rowFormat = "%-" + maxIdKeyLength + "s %8d %15s %15s %15s";
                System.out.println(String.format(rowFormat, 
                    formattedIdKey, stats.count, avgDocSizeStr, avgOplogSizeStr, ratioStr));
            } else {
                // Show simplified information when only oplog sizes are available
                String rowFormat = "%-" + maxIdKeyLength + "s %8d %15s";
                System.out.println(String.format(rowFormat, 
                    formattedIdKey, stats.count, avgOplogSizeStr));
            }
        }
        
        System.out.println();
        System.out.println("Notes:");
        System.out.println("- Count: Total operations on this _id (inserts + updates + deletes)");
        System.out.println("- Avg Oplog Size: Average oplog entry size (small for updates/deletes, full for inserts)");
        System.out.println("- ID Statistics Threshold: " + idStatsThreshold + " bytes (only documents >= this size are tracked)");
        
        if (fetchDocSizes) {
            System.out.println("- Avg Doc Size: Average actual document size (excludes deletes where size is unknown)");
            System.out.println("- Doc/Oplog Ratio: Higher ratio indicates documents much larger than their oplog entries");
        } else {
            System.out.println("- Use --fetchDocSizes option to see actual document sizes (slower but more accurate)");
        }
    }
    
    protected void stop() {
        shutdown = true;
    }
}