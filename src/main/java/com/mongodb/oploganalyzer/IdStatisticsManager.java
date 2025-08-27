package com.mongodb.oploganalyzer;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bson.BsonValue;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.mongodb.util.bson.BsonValueConverter;

/**
 * Shared utility for managing and reporting ID statistics across scan and tail commands
 */
public class IdStatisticsManager {
    
    private final Cache<String, IdStatistics> idStatsCache;
    private final long idStatsThreshold;
    private final int topIdCount;
    
    public IdStatisticsManager(long idStatsThreshold, int topIdCount) {
        this.idStatsThreshold = idStatsThreshold;
        this.topIdCount = topIdCount;
        this.idStatsCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .build();
    }
    
    /**
     * Statistics for a specific document ID across all operations
     */
    static class IdStatistics {
        long count = 0;
        long totalDocSize = 0;
        long docSizeCount = 0;  // Count of entries where we have doc size (not -1)
        long totalOplogSize = 0;
        
        synchronized void addSizes(long docSize, long oplogSize) {
            count++;
            if (docSize >= 0) {
                docSizeCount++;
                totalDocSize += docSize;
            }
            totalOplogSize += oplogSize;
        }
        
        synchronized void addOplogOnly(long oplogSize) {
            addSizes(-1, oplogSize);
        }
        
        double getAverageDocSize() {
            return docSizeCount > 0 ? (double)totalDocSize / docSizeCount : 0;
        }
        
        double getAverageOplogSize() {
            return count > 0 ? (double)totalOplogSize / count : 0;
        }
    }
    
    /**
     * Add statistics for a document ID
     */
    public void addIdStatistics(String namespace, BsonValue id, long docSize, long oplogSize) {
        if (docSize < idStatsThreshold && oplogSize < idStatsThreshold) {
            return; // Below threshold, don't track
        }
        
        String idKey = namespace + "::" + getIdString(id);
        IdStatistics stats = idStatsCache.get(idKey, k -> new IdStatistics());
        stats.addSizes(docSize, oplogSize);
    }
    
    /**
     * Add statistics for a document ID (oplog size only)
     */
    public void addIdStatisticsOplogOnly(String namespace, BsonValue id, long oplogSize) {
        if (oplogSize < idStatsThreshold) {
            return; // Below threshold, don't track
        }
        
        String idKey = namespace + "::" + getIdString(id);
        IdStatistics stats = idStatsCache.get(idKey, k -> new IdStatistics());
        stats.addOplogOnly(oplogSize);
    }
    
    /**
     * Print ID statistics report
     */
    public void printIdStatistics(boolean fetchDocSizes) {
        System.out.println();
        System.out.println("Top " + topIdCount + " most frequent _id values:");
        
        Map<String, IdStatistics> statsMap = idStatsCache.asMap();
        List<Map.Entry<String, IdStatistics>> sortedEntries = statsMap.entrySet().stream()
            .sorted((e1, e2) -> Long.compare(e2.getValue().count, e1.getValue().count))
            .limit(topIdCount)
            .collect(Collectors.toList());
        
        if (sortedEntries.isEmpty()) {
            System.out.println("No ID statistics available (no documents exceeded the ID statistics threshold)");
            return;
        }
        
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
    
    private String getIdString(BsonValue id) {
        if (id == null) {
            return "null";
        }
        
        // Use BsonValueConverter to get clean representation
        Object converted = BsonValueConverter.convertBsonValueToObject(id);
        if (converted != null) {
            return converted.toString();
        } else {
            return "null";
        }
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
    
    /**
     * Check if we have any statistics collected
     */
    public boolean hasStatistics() {
        return !idStatsCache.asMap().isEmpty();
    }
}