package com.mongodb.oploganalyzer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.shardsync.ShardClient;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "scan", description = "Scan oplog entries within a time range")
public class ScanCommand extends BaseOplogCommand {
    
    private static Logger logger = LoggerFactory.getLogger(ScanCommand.class);
    
    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm.ssZ");
    
    // URI and threshold options inherited from BaseOplogCommand
    
    @Option(names = {"-s", "--startTime"}, description = "Start time (yyyy-MM-ddTHH:mm.sssZ)")
    private String startTimeStr;
    
    @Option(names = {"-e", "--endTime"}, description = "End time (yyyy-MM-ddTHH:mm.sssZ)")
    private String endTimeStr;
    
    @Option(names = {"-d", "--db"}, description = "Database name to query (default: local)", defaultValue = "local")
    private String dbName = "local";
    
    @Option(names = {"-x", "--sheet"}, description = "Excel sheet name")
    private String sheetName;
    
    // Shard and ID stats options inherited from BaseOplogCommand
    // accumulators, idStatisticsManager, shardClient, and stop inherited from BaseOplogCommand
    private Date startTime;
    private Date endTime;
    private File file;
    private FileInputStream fis;

    @Override
    public Integer call() {
        try {
            validateInput();
            initializeClient();
            parseTimeOptions();
            setupSheetName();
            setupShutdownHook();
            
            if (inputFile != null) {
                // Process from BSON file
                processBsonFile();
            } else {
                // Process from MongoDB
                process();
            }
            
            report();
            return 0;
        } catch (IOException e) {
            logger.error("Error processing oplog", e);
            return 1;
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
    
    private void initializeClient() {
        initializeCommon();
    }
    
    private void parseTimeOptions() {
        if (startTimeStr != null) {
            startTime = parseDate(startTimeStr);
        }
        if (endTimeStr != null) {
            endTime = parseDate(endTimeStr);
        }
    }
    
    private void setupSheetName() {
        if (sheetName == null) {
            if (startTime != null) {
                sheetName = String.format("%s %s", dbName, simpleDateFormat.format(startTime));
            } else if (dbName != null) {
                sheetName = dbName;
            } else {
                sheetName = "sheet1";
            }
        }
    }
    
    // setupShutdownHook() method inherited from BaseOplogCommand
    
    private static Date parseDate(String s) {
        TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(s);
        Instant i = Instant.from(ta);
        return Date.from(i);
    }
    
    public void process() throws IOException {
        if (shardClient.isMongos()) {
            System.out.println("Detected sharded cluster - using multi-threaded shard analysis");
            processShardedCluster();
        } else {
            System.out.println("Detected replica set - using single-threaded analysis");
            processReplicaSet();
        }
    }
    
    private void processReplicaSet() throws IOException {
        MongoClient mongoClient = shardClient.getMongoClient();
        processSingleShard("replica-set", mongoClient);
    }
    
    private void processShardedCluster() throws IOException {
        Map<String, MongoClient> shardClients = shardClient.getShardMongoClients();
        List<String> targetShards = getTargetShards(shardClients);
        
        if (targetShards.size() == 1) {
            String shardId = targetShards.get(0);
            MongoClient mongoClient = shardClients.get(shardId);
            System.out.println(String.format("Analyzing single shard: %s", shardId));
            processSingleShard(shardId, mongoClient);
        } else {
            System.out.println(String.format("Analyzing %d shards in parallel", targetShards.size()));
            processMultipleShards(shardClients, targetShards);
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
    
    private void processMultipleShards(Map<String, MongoClient> shardClients, List<String> targetShards) {
        ExecutorService executor = Executors.newFixedThreadPool(targetShards.size());
        List<Future<Map<OplogEntryKey, EntryAccumulator>>> futures = new java.util.ArrayList<>();
        
        for (String shardId : targetShards) {
            MongoClient mongoClient = shardClients.get(shardId);
            Future<Map<OplogEntryKey, EntryAccumulator>> future = executor.submit(() -> {
                Map<OplogEntryKey, EntryAccumulator> shardAccumulators = new HashMap<>();
                try {
                    processShardOplog(shardId, mongoClient, shardAccumulators);
                } catch (Exception e) {
                    logger.error("Error processing shard {}: {}", shardId, e.getMessage(), e);
                }
                return shardAccumulators;
            });
            futures.add(future);
        }
        
        long totalCount = 0;
        for (int i = 0; i < futures.size(); i++) {
            try {
                String shardId = targetShards.get(i);
                Map<OplogEntryKey, EntryAccumulator> shardResults = futures.get(i).get();
                totalCount += mergeResults(shardResults);
                System.out.println(String.format("Completed analysis of shard: %s", shardId));
            } catch (Exception e) {
                logger.error("Error getting results from shard thread: {}", e.getMessage(), e);
            }
        }
        
        executor.shutdown();
        System.out.println(String.format("Total processed across all shards: %,d entries", totalCount));
    }
    
    private void processSingleShard(String shardId, MongoClient mongoClient) throws IOException {
        processShardOplog(shardId, mongoClient, accumulators);
    }
    
    private void processShardOplog(String shardId, MongoClient mongoClient, Map<OplogEntryKey, EntryAccumulator> targetAccumulators) throws IOException {
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection<RawBsonDocument> oplog = db.getCollection("oplog.rs", RawBsonDocument.class);

        BsonTimestamp endTimestamp = null;
        BsonTimestamp startTimestamp = null;
        
        if (startTime != null) {
            long startSeconds = startTime.getTime() / 1000;
            startTimestamp = new BsonTimestamp((int) startSeconds, 1);
        }
        
        if (endTime != null) {
            long endSeconds = endTime.getTime() / 1000;
            endTimestamp = new BsonTimestamp((int) endSeconds, 1);
        }

        Document query = new Document();
        
        if (startTimestamp != null && endTimestamp != null) {
            query.put("ts", new Document("$gte", startTimestamp).append("$lte", endTimestamp));
        } else if (startTimestamp != null) {
            query.put("ts", new Document("$gte", startTimestamp));
        } else if (endTimestamp != null) {
            query.put("ts", new Document("$lte", endTimestamp));
        }

        MongoCursor<RawBsonDocument> cursor = oplog.find(query).iterator();

        long count = 0;
        try {
            while (cursor.hasNext() && !stop) {
                RawBsonDocument doc = cursor.next();
                
                // Process the oplog entry using base class method
                processOplogEntryWithApplyOps(doc, shardId, targetAccumulators);
                count++;
                
                if (count % 10000 == 0) {
                    System.out.println(String.format("[%s] Processed %,d entries", shardId, count));
                }
            }
        } finally {
            cursor.close();
        }
        
        System.out.println(String.format("[%s] Total processed: %,d entries", shardId, count));
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
            
            for (long i = 0; i < shardAcc.getCount(); i++) {
                globalAcc.addExecution(shardAcc.getTotal() / shardAcc.getCount());
            }
        }
        return totalEntries;
    }
    
    /**
     * Process oplog entry with support for applyOps (transactions/bulk ops)
     */
    private void processOplogEntryWithApplyOps(RawBsonDocument doc, String shardId, Map<OplogEntryKey, EntryAccumulator> targetAccumulators) {
        long docSize = doc.getByteBuffer().remaining();
        String ns = doc.getString("ns").getValue();
        String opType = doc.getString("op").getValue();
        
        // Handle applyOps operations (transactions/bulk ops)
        if ("c".equals(opType) && ns.endsWith(".$cmd")) {
            BsonDocument o = doc.getDocument("o");
            if (o != null && o.containsKey("applyOps")) {
                BsonArray applyOps = o.getArray("applyOps");
                for (BsonValue applyOp : applyOps) {
                    if (applyOp.isDocument()) {
                        BsonDocument innerOp = applyOp.asDocument();
                        String innerNs = innerOp.getString("ns", new BsonString("unknown")).getValue();
                        String innerOpType = innerOp.getString("op", new BsonString("unknown")).getValue();
                        
                        if (!innerNs.startsWith("config.")) {
                            OplogEntryKey innerKey = new OplogEntryKey(innerNs, innerOpType);
                            EntryAccumulator innerAcc = targetAccumulators.get(innerKey);
                            if (innerAcc == null) {
                                innerAcc = new EntryAccumulator(innerKey);
                                targetAccumulators.put(innerKey, innerAcc);
                            }
                            innerAcc.addExecution(docSize / applyOps.size());
                        }
                    }
                }
            }
        }
        
        // Process with base class method (handles accumulator, ID stats, threshold logging)
        processOplogEntry(doc, shardId);
    }
    
    // report() method inherited from BaseOplogCommand
    // stop() method inherited from BaseOplogCommand
}