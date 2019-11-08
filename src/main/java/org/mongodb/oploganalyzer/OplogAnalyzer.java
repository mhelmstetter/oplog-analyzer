package org.mongodb.oploganalyzer;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class OplogAnalyzer {
    
    private Map<OplogEntryKey, EntryAccumulator> accumulators = new HashMap<OplogEntryKey, EntryAccumulator>();
    private MongoClient mongoClient;
    private boolean stop = false;
    private Long threshold;

    public OplogAnalyzer(String uri, Long threshold) {
        mongoClient = new MongoClient(new MongoClientURI(uri));
        this.threshold = threshold;
    }

    public void process() {
        
        MongoDatabase db = mongoClient.getDatabase("local");
        MongoCollection<RawBsonDocument> oplog = db.getCollection("oplog.rs", RawBsonDocument.class);

        RawBsonDocument lastOplogEntry = oplog.find().sort(new Document("$natural", -1)).first();

        BsonTimestamp lastTimestamp = (BsonTimestamp) lastOplogEntry.get("ts");

        System.out.println(lastTimestamp);

        Document query = new Document("ts", new Document("$lt", lastTimestamp));
        for (RawBsonDocument doc : oplog.find(query).noCursorTimeout(true)) {
            
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
                long len = doc.getByteBuffer().asNIO().array().length;
                if (len >= threshold) {
                    BsonDocument o = (BsonDocument)doc.get("o");
                    if (o != null) {
                        BsonValue id = o.get("_id");
                        if (id != null) {
                            System.out.println(String.format("%s doc exceeded threshold: %s", ns, id));
                        } else {
                            System.out.println("doc exceeded threshold, but no _id in the 'o' field");
                        }
                        
                    } else {
                        System.out.println("doc exceeded threshold, but no 'o' field in the olog record");
                    }
                    
                }
                accum.addExecution(len);
            }

            if (stop) {
                //mongoClient.close();
                System.out.println(accumulators.size());
                report();
                break;
            }
        }
    }
    
    protected void stop() {
        this.stop = true;
    }
    
    public void report() {
        //System.out.println(String.format("%-55s %-15s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %12s %12s %10s %10s", "Namespace", "operation", "count", "min_ms", "max_ms", "avg_ms", "95%_ms", "total_s", "avgKeysEx", "avgDocsEx", "95%_keysEx", "95%_DocsEx", "totKeysEx(K)", "totDocsEx(K)", "avgReturn", "exRetRatio"));
        System.out.println(String.format("%-55s %5s %10s %10s %10s %10s %20s", "Namespace", "op", "count", "min", "max", "avg", "total"));
        for (EntryAccumulator acc : accumulators.values()) {
            System.out.println(acc);
        }
        
    }
    
    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("connection uri")
                .hasArgs()
                .isRequired()
                .withDescription(  "mongodb connection string uri" )
                .withLongOpt("uri")
                .create( "c" ));
        options.addOption(OptionBuilder.withArgName("log ops size threshold (bytes)")
                .hasArgs()
                .withDescription(  "log operations >= this size" )
                .withLongOpt("threshold")
                .create( "t" ));

        CommandLineParser parser = new GnuParser();
        CommandLine line = null;
        
        try {
            line = parser.parse( options, args );
            
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            printHelpAndExit(options);
        } catch (Exception e) {
            e.printStackTrace();
            printHelpAndExit(options);
        }
        return line;
    }
    
    private static void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "loader", options );
        System.exit(-1);
    }

    public static void main(String[] args) throws UnknownHostException {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        String uri = line.getOptionValue("c");
        String thresholdStr = line.getOptionValue("t");
        Long threshold = null;
        if (thresholdStr != null) {
            threshold = Long.parseLong(thresholdStr);
        }
        final OplogAnalyzer analyzer = new OplogAnalyzer(uri, threshold);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                System.out.println("**** SHUTDOWN *****");
                analyzer.stop();
            }
        }));
        analyzer.process();
        analyzer.report();
    }


}
