package com.mongodb.oploganalyzer;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Projections.include;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class TailingOplogAnalyzer {
	
	protected static final Logger logger = LoggerFactory.getLogger(OplogAnalyzer.class);
	
	private MongoClient mongoClient;
	private boolean shutdown = false;
	private Integer limit;
	private Map<OplogEntryKey, EntryAccumulator> accumulators = new HashMap<OplogEntryKey, EntryAccumulator>();
	
	
	public TailingOplogAnalyzer(String uri) {
		mongoClient = new MongoClient(new MongoClientURI(uri));
	}

	public void run() {

		MongoDatabase local = mongoClient.getDatabase("local");
		MongoCursor<RawBsonDocument> cursor = null;
		MongoCollection<RawBsonDocument> oplog = local.getCollection("oplog.rs", RawBsonDocument.class);

		//BsonTimestamp shardTimestamp = getLatestOplogTimestamp();
		//Bson query = gte("ts", shardTimestamp);
		
		int i = 0;
		try {
			cursor = oplog.find().sort(new Document("$natural", 1)).noCursorTimeout(true)
					.cursorType(CursorType.TailableAwait).iterator();
			while (cursor.hasNext() && !shutdown) {

				RawBsonDocument doc = cursor.next();

				String ns = ((BsonString) doc.get("ns")).getValue();
				BsonString op = (BsonString) doc.get("op");
				String opType = op.getValue();
				
	            if (opType.equals("n") || ns.startsWith("config.")) {
	            	continue;
	            }

			

				OplogEntryKey key = new OplogEntryKey(ns, opType);
				EntryAccumulator accum = accumulators.get(key);
				if (accum == null) {
					accum = new EntryAccumulator(key);
					accumulators.put(key, accum);
				}

				long len = doc.getByteBuffer().asNIO().array().length;
				accum.addExecution(len);
				if (i++ % 5000 == 0) {
					System.out.print(".");
				}
				
				if (limit != null && i >= limit) {
					stop();
                    report();
				}
			}

		} catch (MongoInterruptedException e) {
			// ignore
		} catch (Exception e) {
			logger.error("tail error", e);
		} finally {
			try {
				cursor.close();
			} catch (Exception e) {
			}
		}

	}
	
    public void report() {
        //System.out.println(String.format("%-55s %-15s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %12s %12s %10s %10s", "Namespace", "operation", "count", "min_ms", "max_ms", "avg_ms", "95%_ms", "total_s", "avgKeysEx", "avgDocsEx", "95%_keysEx", "95%_DocsEx", "totKeysEx(K)", "totDocsEx(K)", "avgReturn", "exRetRatio"));
        System.out.println();
    	System.out.println(String.format("%-80s %5s %10s %10s %10s %10s %20s", "Namespace", "op", "count", "min", "max", "avg", "total"));
        for (EntryAccumulator acc : accumulators.values()) {
            System.out.println(acc);
        }
        
    }
    
    private BsonTimestamp getLatestOplogTimestamp() {
		MongoCollection<Document> coll = mongoClient.getDatabase("local").getCollection("oplog.rs");
		Document doc = null;
		doc = coll.find().comment("getLatestOplogTimestamp").projection(include("ts")).sort(eq("$natural", -1)).first();
		BsonTimestamp ts = (BsonTimestamp) doc.get("ts");
		return ts;
	}
	
    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("connection uri")
                .hasArg()
                .isRequired()
                .withDescription(  "mongodb connection string uri" )
                .withLongOpt("uri")
                .create( "c" ));
        options.addOption(OptionBuilder.withArgName("limit")
                .hasArgs()
                .withDescription(  "only examine limit number of oplog entries" )
                .withLongOpt("limit")
                .create( "l" ));

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
	
    public static void main(String[] args) throws java.text.ParseException, IOException, InvalidFormatException {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        String uri = line.getOptionValue("c");
        
        
        final TailingOplogAnalyzer analyzer = new TailingOplogAnalyzer(uri);
        
        if (line.hasOption("l")) {
        	String limitStr = line.getOptionValue("l");
        	analyzer.setLimit(Integer.parseInt(limitStr));
        	
        } else {
        	Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                public void run() {
                	System.out.println();
                    System.out.println("**** SHUTDOWN *****");
                    analyzer.stop();
                    analyzer.report();
                }
            }));
        }
        
        analyzer.run();
    }

	protected void stop() {
		shutdown = true;
		
	}

	public void setLimit(Integer limit) {
		this.limit = limit;
	}

}
