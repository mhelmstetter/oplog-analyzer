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
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.RawBsonDocument;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class OplogAnalyzer {
    
    private Map<OplogEntryKey, EntryAccumulator> accumulators = new HashMap<OplogEntryKey, EntryAccumulator>();
    private MongoClient mongoClient;

    public OplogAnalyzer(String uri) {
        mongoClient = new MongoClient(new MongoClientURI(uri));
    }

    public void process() {
        
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
    
    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("connection uri")
                .hasArgs()
                .isRequired()
                .withDescription(  "mongodb connection string uri" )
                .withLongOpt("uri")
                .create( "c" ));

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
        OplogAnalyzer analyzer = new OplogAnalyzer(uri);
        analyzer.process();
        analyzer.report();
    }
}
