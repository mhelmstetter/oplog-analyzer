package com.mongodb.oploganalyzer;

import org.apache.commons.io.FileUtils;
import java.text.NumberFormat;
import java.util.Locale;

public class EntryAccumulator {
    
    private OplogEntryKey key;
    
    private long count;
    private long total;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    
    public EntryAccumulator(OplogEntryKey key) {
        this.key = key;
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
    }
    
    public String toString() {
        NumberFormat nf = NumberFormat.getInstance(Locale.US);
        String totalWithSize = String.format("%s (%s)", 
            nf.format(total), 
            FileUtils.byteCountToDisplaySize(total));
        return String.format("%-80s %5s %15s %15s %15s %15s %30s", 
            key.ns, 
            key.op, 
            nf.format(count), 
            nf.format(min), 
            nf.format(max), 
            nf.format(total/count), 
            totalWithSize);
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
