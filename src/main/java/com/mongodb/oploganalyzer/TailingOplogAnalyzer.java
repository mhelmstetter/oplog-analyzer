package com.mongodb.oploganalyzer;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
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

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class TailingOplogAnalyzer {
	
	protected static final Logger logger = LoggerFactory.getLogger(OplogAnalyzer.class);
	
	private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH.mm");
	
	private MongoClient mongoClient;
	private boolean shutdown = false;
	private Integer limit;
	private Long threshold;
	private Map<OplogEntryKey, EntryAccumulator> accumulators = new HashMap<OplogEntryKey, EntryAccumulator>();
	
	private boolean dump;
	FileChannel channel;
	
	
	public TailingOplogAnalyzer(String uri) {
		mongoClient = new MongoClient(new MongoClientURI(uri));
	}
	
	private void debug(String ns, BsonValue id) {
        String idVal = null;
        if (id.getBsonType().equals(BsonType.BINARY)) {
            BsonBinary b = (BsonBinary)id;
            JsonWriter jsonWriter = new JsonWriter(new StringWriter(), new JsonWriterSettings(JsonMode.SHELL, false));
            jsonWriter.writeBinaryData(b);
            idVal = jsonWriter.getWriter().toString();
        } else {
            idVal = id.toString();
        }
        System.out.println(String.format("%s doc exceeded threshold: {_id: %s }", ns, idVal));
    }

	public void run() throws FileNotFoundException {
		
		if (dump) {
			initDumpFile();
		}

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
				
				if (opType.equals("d")) {
					System.out.println(doc);
				}
				
	            if (ns.startsWith("config.")) {
	            	continue;
	            }
	            
	            if (channel != null) {
					ByteBuffer buffer = doc.getByteBuffer().asNIO();
	                channel.write(buffer);
				}

				OplogEntryKey key = new OplogEntryKey(ns, opType);
				EntryAccumulator accum = accumulators.get(key);
				if (accum == null) {
					accum = new EntryAccumulator(key);
					accumulators.put(key, accum);
				}

				long len = doc.getByteBuffer().asNIO().array().length;
	            if (len >= threshold) {
	                BsonDocument o = (BsonDocument)doc.get("o");
	                BsonDocument o2 = (BsonDocument)doc.get("o2");
	                if (o2 != null) {
	                    BsonValue id = o2.get("_id");
	                    if (id != null) {
	                        debug(ns, id);
	                    } else {
	                        System.out.println("doc exceeded threshold, but no _id in the 'o2' field");
	                    }
	                } else if (o != null) {
	                    BsonValue id = o.get("_id");
	                    if (id != null) {
	                        debug(ns, id);
	                    } else {
	                        System.out.println("doc exceeded threshold, but no _id in the 'o' field");
	                    }
	                    
	                } else {
	                    System.out.println("doc exceeded threshold, but no 'o' or 'o2' field in the olog record");
	                }
	                
	            }
	            
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
			if (channel != null) {
				try {
					channel.close();
				} catch (IOException e) {
				}
			}
		}

	}
	
    private void initDumpFile() throws FileNotFoundException {
    	File outputFile = new File("oplogDump_" + dateFormat.format(new Date()) + ".bson");
        FileOutputStream fos = new FileOutputStream(outputFile);
        channel = fos.getChannel();
		
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
        options.addOption(OptionBuilder.withArgName("log ops size threshold (bytes)")
                .hasArgs()
                .withDescription(  "log operations >= this size" )
                .withLongOpt("threshold")
                .create( "t" ));
        options.addOption(OptionBuilder.withArgName("limit")
                .hasArgs()
                .withDescription(  "only examine limit number of oplog entries" )
                .withLongOpt("limit")
                .create( "l" ));
        options.addOption(OptionBuilder.withArgName("dump")
                .withDescription(  "dump bson to output file (mongodump format)" )
                .withLongOpt("dump")
                .create( "d" ));

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
        
        String thresholdStr = line.getOptionValue("t");
        Long threshold = Long.MAX_VALUE;
        if (thresholdStr != null) {
            threshold = Long.parseLong(thresholdStr);
        }
        analyzer.setThreshold(threshold);
        
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
        
        if (line.hasOption("d")) {
        	analyzer.setDump(true);
        }
        
        analyzer.run();
    }

	protected void stop() {
		shutdown = true;
		
	}

	public void setLimit(Integer limit) {
		this.limit = limit;
	}

	public void setThreshold(Long threshold) {
		this.threshold = threshold;
	}

	public void setDump(boolean dump) {
		this.dump = dump;
	}

}
