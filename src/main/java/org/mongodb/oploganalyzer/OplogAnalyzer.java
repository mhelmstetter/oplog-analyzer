package org.mongodb.oploganalyzer;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.RawBsonDocument;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class OplogAnalyzer {
    
    private Map<OplogEntryKey, EntryAccumulator> accumulators = new HashMap<OplogEntryKey, EntryAccumulator>();

    public void process() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("local");
        MongoCollection<RawBsonDocument> oplog = db.getCollection("oplog.rs", RawBsonDocument.class);

        RawBsonDocument lastOplogEntry = oplog.find().sort(new Document("$natural", -1)).first();

        BsonTimestamp lastTimestamp = (BsonTimestamp) lastOplogEntry.get("ts");

        System.out.println(lastTimestamp);

        Document query = new Document("ts", new Document("$lt", lastTimestamp));
        for (RawBsonDocument doc : oplog.find(query)) {
            BsonString ns = (BsonString) doc.get("ns");
            BsonString op = (BsonString) doc.get("op");

            // ignore no-op
            if (!op.getValue().equals("n")) {
                OplogEntryKey key = new OplogEntryKey(ns.getValue(), op.getValue());
                EntryAccumulator accum = accumulators.get(key);
                if (accum == null) {
                    accum = new EntryAccumulator(key);
                    accumulators.put(key, accum);
                }
                accum.addExecution(doc.size());
            }

            //System.out.println(ns + " " + doc.size());
        }
    }
    
    public void report() {
        //System.out.println(String.format("%-55s %-15s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %12s %12s %10s %10s", "Namespace", "operation", "count", "min_ms", "max_ms", "avg_ms", "95%_ms", "total_s", "avgKeysEx", "avgDocsEx", "95%_keysEx", "95%_DocsEx", "totKeysEx(K)", "totDocsEx(K)", "avgReturn", "exRetRatio"));
        System.out.println(String.format("%-55s %5s %10s %10s %10s", "Namespace", "op", "min", "max", "avg"));
        for (EntryAccumulator acc : accumulators.values()) {
            System.out.println(acc);
        }
        
    }

    public static void main(String[] args) throws UnknownHostException {
        OplogAnalyzer analyzer = new OplogAnalyzer();
        analyzer.process();
        analyzer.report();
    }
}
