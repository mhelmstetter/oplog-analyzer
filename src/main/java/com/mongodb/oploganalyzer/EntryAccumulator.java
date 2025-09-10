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
        count++;
        total += amt;
        if (amt > max) {
            max = amt;
        }
        if (amt < min) {
            min = amt;
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
        
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-" + namespaceWidth + "s %2s %10s %10s %10s %10s %12s", 
            truncateNamespace(key.ns, namespaceWidth), 
            key.op, 
            nf.format(count), 
            nf.format(min), 
            nf.format(max), 
            nf.format(total/count), 
            totalSize));
        
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
    
    public static String getHeaderFormat(List<Long> thresholdBuckets) {
        return getHeaderFormat(thresholdBuckets, 50); // Default namespace width
    }
    
    public static String getHeaderFormat(List<Long> thresholdBuckets, int namespaceWidth) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-" + namespaceWidth + "s %2s %10s %10s %10s %10s %12s", 
            "Namespace", "op", "count", "min", "max", "avg", "total size"));
        
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
        
        // Merge threshold bucket counts
        if (this.thresholdCounts != null && other.thresholdCounts != null 
            && this.thresholdCounts.size() == other.thresholdCounts.size()) {
            for (int i = 0; i < this.thresholdCounts.size(); i++) {
                this.thresholdCounts.set(i, this.thresholdCounts.get(i) + other.thresholdCounts.get(i));
            }
        }
    }



}
