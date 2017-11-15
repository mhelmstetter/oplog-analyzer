package org.mongodb.oploganalyzer;

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
        return String.format("%-55s %5s %10d %10d %10d %10d %20d", key.ns, key.op, count, min, max, total/count, total);
        
    }

    public long getCount() {
        return count;
    }
    
    public long getMax() {
        return max;
    }

    public long getAvg() {
        return total/count;
    }



}
