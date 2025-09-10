package com.mongodb.oploganalyzer;

import org.apache.commons.io.FileUtils;
import java.text.NumberFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.Locale;

public class EntryAccumulator {
    
    private OplogEntryKey key;
    
    private long count;
    private long total;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    
    private List<Long> thresholdBuckets;
    private List<Long> thresholdCounts;
    
    // Element and diff field tracking
    private long totalElements = 0;
    private long totalDiffFields = 0;
    private long documentsWithElements = 0;
    private long documentsWithDiff = 0;
    
    public EntryAccumulator(OplogEntryKey key) {
        this(key, new ArrayList<>());
    }
    
    public EntryAccumulator(OplogEntryKey key, List<Long> thresholdBuckets) {
        this.key = key;
        this.thresholdBuckets = new ArrayList<>(thresholdBuckets);
        this.thresholdCounts = new ArrayList<>();
        // Initialize threshold counts to 0
        for (int i = 0; i < thresholdBuckets.size(); i++) {
            this.thresholdCounts.add(0L);
        }
    }

    public void addExecution(long amt) {
        addExecution(amt, 0, 0);
    }
    
    public void addExecution(long amt, int elementCount, int diffFieldCount) {
        count++;
        total += amt;
        if (amt > max) {
            max = amt;
        }
        if (amt < min) {
            min = amt;
        }
        
        // Track element and diff field statistics
        if (elementCount > 0) {
            totalElements += elementCount;
            documentsWithElements++;
        }
        if (diffFieldCount > 0) {
            totalDiffFields += diffFieldCount;
            documentsWithDiff++;
        }
        
        // Update threshold bucket counts
        for (int i = 0; i < thresholdBuckets.size(); i++) {
            if (amt > thresholdBuckets.get(i)) {
                thresholdCounts.set(i, thresholdCounts.get(i) + 1);
            }
        }
    }
    
    public String toString() {
        return toString(50); // Default namespace width
    }
    
    public String toString(int namespaceWidth) {
        NumberFormat nf = NumberFormat.getInstance(Locale.US);
        String totalSize = FileUtils.byteCountToDisplaySize(total);
        String minSize = formatSizeWithPrecision(min);
        String maxSize = formatSizeWithPrecision(max);
        String avgSize = formatSizeWithPrecision(total/count);
        
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-" + namespaceWidth + "s %2s %10s %10s %10s %10s %12s", 
            truncateNamespace(key.ns, namespaceWidth), 
            key.op, 
            nf.format(count), 
            minSize, 
            maxSize, 
            avgSize, 
            totalSize));
        
        // Add element and diff field statistics
        if (documentsWithElements > 0) {
            double avgElements = (double) totalElements / documentsWithElements;
            sb.append(String.format(" %10.1f", avgElements));
        } else {
            sb.append(String.format(" %10s", "N/A"));
        }
        
        if (documentsWithDiff > 0) {
            double avgDiffFields = (double) totalDiffFields / documentsWithDiff;
            sb.append(String.format(" %10.1f", avgDiffFields));
        } else {
            sb.append(String.format(" %10s", "N/A"));
        }
        
        // Add threshold bucket columns
        for (Long thresholdCount : thresholdCounts) {
            sb.append(String.format(" %10s", nf.format(thresholdCount)));
        }
        
        return sb.toString();
    }
    
    private String truncateNamespace(String namespace, int maxWidth) {
        if (namespace.length() <= maxWidth) {
            return namespace;
        }
        // Truncate from the middle, keeping beginning and end
        int keepStart = Math.max(15, maxWidth / 3);
        int keepEnd = Math.max(10, maxWidth / 4);
        if (keepStart + keepEnd + 3 >= maxWidth) {
            return namespace.substring(0, maxWidth - 3) + "...";
        }
        return namespace.substring(0, keepStart) + "..." + namespace.substring(namespace.length() - keepEnd);
    }
    
    private String formatSizeWithPrecision(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        } else {
            return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
        }
    }
    
    public static String getHeaderFormat(List<Long> thresholdBuckets) {
        return getHeaderFormat(thresholdBuckets, 50); // Default namespace width
    }
    
    public static String getHeaderFormat(List<Long> thresholdBuckets, int namespaceWidth) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-" + namespaceWidth + "s %2s %10s %10s %10s %10s %12s", 
            "Namespace", "op", "count", "min", "max", "avg", "total size"));
        
        // Add element and diff field headers
        sb.append(String.format(" %10s %10s", "avg elems", "avg diffs"));
        
        // Add threshold bucket headers
        for (Long threshold : thresholdBuckets) {
            String thresholdLabel = String.format("> %s", FileUtils.byteCountToDisplaySize(threshold));
            sb.append(String.format(" %10s", thresholdLabel));
        }
        
        return sb.toString();
    }
    
    public static String getSeparatorLine(List<Long> thresholdBuckets, int namespaceWidth) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-" + namespaceWidth + "s %2s %10s %10s %10s %10s %12s", 
            "=".repeat(namespaceWidth), "==", "==========", "==========", "==========", "==========", "============"));
        
        // Add element and diff field separators
        sb.append(String.format(" %10s %10s", "==========", "=========="));
        
        // Add threshold bucket separators
        for (Long threshold : thresholdBuckets) {
            sb.append(String.format(" %10s", "=========="));
        }
        
        return sb.toString();
    }

    public long getCount() {
        return count;
    }
    
    public long getMin() {
        return min;
    }
    
    public long getMax() {
        return max;
    }

    public long getAvg() {
        return total/count;
    }
    
    public long getTotal() {
        return total;
    }
    
    public String getNamespace() {
    	return key.ns;
    }
    
    public String getOp() {
    	return key.op;
    }
    
    /**
     * Merge another accumulator's data into this one
     */
    public void merge(EntryAccumulator other) {
        this.count += other.count;
        this.total += other.total;
        this.min = Math.min(this.min, other.min);
        this.max = Math.max(this.max, other.max);
        
        // Merge element and diff field statistics
        this.totalElements += other.totalElements;
        this.totalDiffFields += other.totalDiffFields;
        this.documentsWithElements += other.documentsWithElements;
        this.documentsWithDiff += other.documentsWithDiff;
        
        // Merge threshold bucket counts
        if (this.thresholdCounts != null && other.thresholdCounts != null 
            && this.thresholdCounts.size() == other.thresholdCounts.size()) {
            for (int i = 0; i < this.thresholdCounts.size(); i++) {
                this.thresholdCounts.set(i, this.thresholdCounts.get(i) + other.thresholdCounts.get(i));
            }
        }
    }



}
