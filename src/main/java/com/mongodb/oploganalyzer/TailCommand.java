package com.mongodb.oploganalyzer;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Projections.include;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.bson.BsonArray;
import org.bson.BsonBinary;
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
    
    @Option(names = {"--idStats"}, description = "Enable _id statistics tracking")
    private boolean idStats = false;
    
    @Option(names = {"--topIdCount"}, description = "Number of top frequent _id values to report (default 20)", defaultValue = "20")
    private int topIdCount = 20;
    
    @Option(names = {"--shardIndex"}, description = "Comma-separated list of shard indices to analyze (0,1,2...), default: all shards")
    private String shardIndexes;
    
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
    
    static class IdStatistics {
        long count = 0;
        long minSize = Long.MAX_VALUE;
        long maxSize = Long.MIN_VALUE;
        long totalSize = 0;
        
        void addSize(long size) {
            count++;
            totalSize += size;
            if (size < minSize) minSize = size;
            if (size > maxSize) maxSize = size;
        }
        
        double getAverageSize() {
            return count > 0 ? (double) totalSize / count : 0;
        }
    }
    
    class ShardTailWorker implements Runnable {
        private final String shardId;
        private final MongoClient mongoClient;
        private final Map<OplogEntryKey, EntryAccumulator> targetAccumulators;
        
        private final AtomicBoolean running = new AtomicBoolean();
        private final AtomicBoolean complete = new AtomicBoolean();
        
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
        
        @Override
        public void run() {
            try {
                tailShardOplog();
            } catch (Exception e) {
                logger.error("Error tailing shard {}: {}", shardId, e.getMessage(), e);
            } finally {
                complete.set(true);
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
                                                innerAccum = new EntryAccumulator(innerKey);
                                                targetAccumulators.put(innerKey, innerAccum);
                                            }
                                            // Use a portion of the total doc size for each nested op
                                            innerAccum.addExecution(docSize / applyOps.size());
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Always track the outer operation too
                        OplogEntryKey key = new OplogEntryKey(ns, opType);
                        EntryAccumulator accum = targetAccumulators.get(key);
                        if (accum == null) {
                            accum = new EntryAccumulator(key);
                            targetAccumulators.put(key, accum);
                        }

                        if (docSize >= threshold) {
                            BsonDocument o = (BsonDocument) doc.get("o");
                            BsonDocument o2 = (BsonDocument) doc.get("o2");
                            if (o2 != null) {
                                BsonValue id = o2.get("_id");
                                if (id != null) {
                                    debug(shardId, ns, id, docSize);
                                } else {
                                    System.out.println("doc exceeded threshold, but no _id in the 'o2' field");
                                }
                            } else if (o != null) {
                                BsonValue id = o.get("_id");
                                if (id != null) {
                                    debug(shardId, ns, id, docSize);
                                } else {
                                    System.out.println("doc exceeded threshold, but no _id in the 'o' field");
                                }
                            } else {
                                System.out.println("doc exceeded threshold, but no 'o' or 'o2' field in the olog record");
                            }

                            if (idStats) {
                                BsonValue id = null;
                                // For updates, _id is in o2; for inserts/deletes, it's in o
                                if (o2 != null) {
                                    id = o2.get("_id");
                                } else if (o != null) {
                                    id = o.get("_id");
                                }
                                
                                if (id != null) {
                                    String idKey = ns + "::" + getIdString(id);
                                    IdStatistics stats = idStatsCache.get(idKey, k -> new IdStatistics());
                                    stats.addSize(docSize);
                                }
                            }
                        }

                        accum.addExecution(docSize);
                        
                        if (dump) {
                            writeDump(doc);
                        }
                        
                        count++;
                        
                        // Report every 30 seconds
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastReportTime >= 30000) {
                            long lagSeconds = calculateLagSeconds(lastOplogTimestamp);
                            logger.info("[{}] Processed {} entries, Lag: {}s", 
                                shardId, String.format("%,d", count), lagSeconds);
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
            analyze();
            return 0;
        } catch (IOException e) {
            logger.error("Error analyzing oplog", e);
            return 1;
        }
    }
    
    private void setupShutdownHook() {
        if (limit == null) {
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                public void run() {
                    System.out.println();
                    System.out.println("**** SHUTDOWN *****");
                    stop();
                    
                    // If we have multi-threaded execution, stop workers and merge results
                    if (workers != null && !workers.isEmpty()) {
                        stopWorkers();
                        mergeShardResults();
                    }
                    
                    report();
                }
            }));
        }
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
        
        // Create and start workers for each shard
        for (String shardId : targetShards) {
            MongoClient mongoClient = shardClients.get(shardId);
            
            // Create separate accumulator for this shard - no contention!
            Map<OplogEntryKey, EntryAccumulator> shardAccumulator = new HashMap<>();
            shardAccumulators.add(shardAccumulator);
            
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
        logger.debug("Stopping {} workers gracefully", workers.size());
        for (ShardTailWorker worker : workers) {
            worker.stop();
        }
        
        currentExecutor.shutdown();
        try {
            if (!currentExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                logger.debug("Workers didn't terminate gracefully, forcing shutdown");
                currentExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.debug("Interrupted while waiting for workers to stop");
            currentExecutor.shutdownNow();
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
                globalAcc = new EntryAccumulator(key);
                accumulators.put(key, globalAcc);
            }
            
            // Merge the accumulator data
            for (long i = 0; i < shardAcc.getCount(); i++) {
                globalAcc.addExecution(shardAcc.getTotal() / shardAcc.getCount());
            }
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
    
    private void debug(String shardId, String ns, BsonValue id, long docSize) {
        if (id != null) {
            String idVal = getIdString(id);
            String sizeDisplay = org.apache.commons.io.FileUtils.byteCountToDisplaySize(docSize);
            System.out.println(String.format("[%s] %s doc exceeded threshold (%s): {_id: %s }", 
                shardId, ns, sizeDisplay, idVal));
        }
    }
    
    private String getIdString(BsonValue id) {
        if (id.getBsonType().equals(BsonType.BINARY)) {
            BsonBinary b = (BsonBinary) id;
            try (JsonWriter jsonWriter = new JsonWriter(new StringWriter(), JsonWriterSettings.builder().outputMode(JsonMode.SHELL).indent(false).build())) {
				jsonWriter.writeBinaryData(b);
				return jsonWriter.getWriter().toString();
			}
        } else {
            return id.toString();
        }
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
    
    public void report() {
        System.out.println();
        System.out.println(String.format("%-80s %5s %15s %15s %15s %15s %30s", "Namespace", "op", "count", "min", "max",
                "avg", "total (size)"));
        accumulators.values().stream().sorted(Comparator.comparingLong(EntryAccumulator::getCount).reversed())
                .forEach(acc -> System.out.println(acc));
        
        if (idStats && !idStatsCache.asMap().isEmpty()) {
            printIdStatistics();
        }
    }
    
    private void printIdStatistics() {
        System.out.println();
        System.out.println("Top " + topIdCount + " most frequent _id values:");
        System.out.println(String.format("%-80s %10s %10s %10s %15s", "Namespace::_id", "count", "min", "max", "avg"));
        System.out.println(String.format("%-80s %10s %10s %10s %15s", 
            "=".repeat(80), "=".repeat(10), "=".repeat(10), "=".repeat(10), "=".repeat(15)));
            
        Map<String, IdStatistics> statsMap = idStatsCache.asMap();
        List<Map.Entry<String, IdStatistics>> sortedEntries = statsMap.entrySet().stream()
            .sorted((e1, e2) -> Long.compare(e2.getValue().count, e1.getValue().count))
            .limit(topIdCount)
            .collect(Collectors.toList());
            
        for (Map.Entry<String, IdStatistics> entry : sortedEntries) {
            String idKey = entry.getKey();
            IdStatistics stats = entry.getValue();
            System.out.println(String.format("%-80s %10d %10d %10d %15.2f", 
                idKey, stats.count, stats.minSize, stats.maxSize, stats.getAverageSize()));
        }
    }
    
    protected void stop() {
        shutdown = true;
    }
}