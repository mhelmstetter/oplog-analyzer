package com.mongodb.oploganalyzer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
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
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
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

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class OplogAnalyzer {
	
	private static Logger logger = LoggerFactory.getLogger(OplogAnalyzer.class);
	
	private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm.ssZ");
	
    private Map<OplogEntryKey, EntryAccumulator> accumulators = new HashMap<OplogEntryKey, EntryAccumulator>();
    private MongoClient mongoClient;
    private boolean stop = false;
    private Long threshold;
    private Date startTime;
    private Date endTime;
    private String dbName = "local";
    
    private Workbook workbook;
    private File file;
    FileInputStream fis;
    private String sheetName;

    public OplogAnalyzer(String uri, Long threshold, Date startTime, Date endTime) {
        mongoClient = new MongoClient(new MongoClientURI(uri));
        this.threshold = threshold;
        this.startTime = startTime;
    }

    public void process() throws InvalidFormatException, IOException {
        
        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection<RawBsonDocument> oplog = db.getCollection("oplog.rs", RawBsonDocument.class);

        BsonTimestamp endTimestamp = null;
        if (endTime == null) {
        	RawBsonDocument lastOplogEntry = oplog.find().sort(new Document("$natural", -1)).first();
            endTimestamp = (BsonTimestamp) lastOplogEntry.get("ts");
        } else {
        	logger.debug("endTime: {}", endTime);
        	int epoch = (int)(endTime.getTime() / 1000);
        	endTimestamp = new BsonTimestamp(epoch, 0);
        }
        
        
        BsonTimestamp start = null;
        if (startTime != null) {
        	logger.debug("startTime: {}", startTime);
            int epoch = (int)(startTime.getTime() / 1000);
            start = new BsonTimestamp(epoch, 0);
        }
       

        File currDir = new File(".");
    	String path = currDir.getAbsolutePath();
    	String fileLocation = path.substring(0, path.length() - 1) + "oplog.xlsx";
    	
    	file = new File(fileLocation);
    	
    	if (file.exists()) {
    		FileInputStream fis = new FileInputStream(file);
    		workbook = new XSSFWorkbook(fis);
    	} else {
    		workbook = new XSSFWorkbook();
    	}
        
        Document query = null;
        if (start != null) {
            query = new Document("ts", new Document("$lt", endTimestamp).append("$gt", start));
        } else {
            query = new Document("ts", new Document("$lt", endTimestamp));
        }
        
        logger.debug("query: {}", query);
        for (RawBsonDocument doc : oplog.find(query).noCursorTimeout(true)) {
            
            String ns = ((BsonString) doc.get("ns")).getValue();
            BsonString op = (BsonString) doc.get("op");
            String opType = op.getValue();

            // ignore no-op
            if (opType.equals("n") && ns.equals("")) {
            	continue;
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
            

            if (stop) {
                //mongoClient.close();
                System.out.println(accumulators.size());
                report();
                break;
            }
        }
    }
    
    public void writeExcel() throws IOException {
    	

    	Sheet sheet = workbook.createSheet(sheetName);

    	Row header = sheet.createRow(0);

    	CellStyle headerStyle = workbook.createCellStyle();
    	headerStyle.setFillForegroundColor(IndexedColors.LIGHT_BLUE.getIndex());
    	headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

    	XSSFFont font = ((XSSFWorkbook) workbook).createFont();
    	font.setFontName("Arial");
    	font.setFontHeightInPoints((short) 16);
    	font.setBold(true);
    	headerStyle.setFont(font);

    	int c = 0;
    	Cell headerCell = header.createCell(c++);
    	headerCell.setCellValue("Namespace");
    	headerCell.setCellStyle(headerStyle);

    	headerCell = header.createCell(c++);
    	headerCell.setCellValue("op");
    	headerCell.setCellStyle(headerStyle);
    	
    	headerCell = header.createCell(c++);
    	headerCell.setCellValue("count");
    	headerCell.setCellStyle(headerStyle);
    	
    	headerCell = header.createCell(c++);
    	headerCell.setCellValue("min");
    	headerCell.setCellStyle(headerStyle);
    	
    	headerCell = header.createCell(c++);
    	headerCell.setCellValue("max");
    	headerCell.setCellStyle(headerStyle);
    	
    	headerCell = header.createCell(c++);
    	headerCell.setCellValue("avg");
    	headerCell.setCellStyle(headerStyle);
    	
    	headerCell = header.createCell(c++);
    	headerCell.setCellValue("total");
    	headerCell.setCellStyle(headerStyle);
    	
    	int rowNum = 1;
    	for (EntryAccumulator acc : accumulators.values()) {
    		Row row = sheet.createRow(rowNum++);
    		Cell cell = row.createCell(0);
    		cell.setCellValue(acc.getNamespace());

    		cell = row.createCell(1);
    		cell.setCellValue(acc.getOp());
    		
    		cell = row.createCell(2);
    		cell.setCellValue(acc.getCount());
    		
    		cell = row.createCell(3);
    		cell.setCellValue(acc.getMin());
    		
    		cell = row.createCell(4);
    		cell.setCellValue(acc.getMax());
    		
    		cell = row.createCell(5);
    		cell.setCellValue(acc.getAvg());
    		
    		cell = row.createCell(6);
    		cell.setCellValue(acc.getTotal());
        }

    	FileOutputStream outputStream = new FileOutputStream(file);
    	workbook.write(outputStream);
    	workbook.close();
    	outputStream.close();
    }
    
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
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
    
    protected void stop() {
        this.stop = true;
    }
    
    public void report() {
        //System.out.println(String.format("%-55s %-15s %10s %10s %10s %10s %10s %10s %10s %10s %10s %10s %12s %12s %10s %10s", "Namespace", "operation", "count", "min_ms", "max_ms", "avg_ms", "95%_ms", "total_s", "avgKeysEx", "avgDocsEx", "95%_keysEx", "95%_DocsEx", "totKeysEx(K)", "totDocsEx(K)", "avgReturn", "exRetRatio"));
        System.out.println(String.format("%-80s %5s %10s %10s %10s %10s %20s", "Namespace", "op", "count", "min", "max", "avg", "total"));
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
        options.addOption(OptionBuilder.withArgName("start time")
                .hasArg()
                .withDescription(  "start time (yyyy-MM-ddTHH:mm.sssZ)" )
                .withLongOpt("startTime")
                .create( "s" ));
        options.addOption(OptionBuilder.withArgName("end time")
                .hasArg()
                .withDescription(  "end time (yyyy-MM-ddTHH:mm.sssZ)" )
                .withLongOpt("endTime")
                .create( "e" ));
        options.addOption(OptionBuilder.withArgName("db name")
                .hasArg()
                .withDescription(  "database name to query (default to local)" )
                .withLongOpt("db")
                .create( "d" ));
        options.addOption(OptionBuilder.withArgName("excel sheet name")
                .hasArg()
                .withLongOpt("sheet")
                .create( "x" ));

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
    
    private static Date parseDate(String s) {
        TemporalAccessor ta = DateTimeFormatter.ISO_INSTANT.parse(s);
        Instant i = Instant.from(ta);
        return Date.from(i);
    }

    public static void main(String[] args) throws java.text.ParseException, IOException, InvalidFormatException {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        String uri = line.getOptionValue("c");
        String thresholdStr = line.getOptionValue("t");
        Long threshold = Long.MAX_VALUE;
        if (thresholdStr != null) {
            threshold = Long.parseLong(thresholdStr);
        }
        
        String dbName = line.getOptionValue("d");
        
        Date startTime = null;
        String startTimeStr = line.getOptionValue("s");
        if (startTimeStr != null) {
            startTime = parseDate(startTimeStr);
        }
        
        Date endTime = null;
        String endTimeStr = line.getOptionValue("e");
        if (endTimeStr != null) {
        	endTime = parseDate(endTimeStr);
        }
       
        
        final OplogAnalyzer analyzer = new OplogAnalyzer(uri, threshold, startTime, endTime);
        if (dbName != null) {
        	analyzer.setDbName(dbName);
        }
        
        
        String sheetName = line.getOptionValue("x");
        if (sheetName == null) {
        	if (startTime != null) {
        		sheetName = String.format("%s %s", dbName, simpleDateFormat.format(startTime));
        	} else {
        		sheetName = dbName;
        	}
        }
        analyzer.setSheetName(sheetName);
        
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                System.out.println("**** SHUTDOWN *****");
                analyzer.stop();
            }
        }));
        analyzer.process();
        analyzer.report();
        analyzer.writeExcel();
    }

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}


}
