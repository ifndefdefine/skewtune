package org.apache.hadoop.mapred;

import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * internal class that keeps track of progress of a merger
 * @author yongchul
 *
 */
public class ReduceStatTracker {
    public static class Stats {
        final long totalBytes;
        volatile long totalProcessed;
        volatile long reduceGroups;
        volatile long reduceValues;
        
        Stats(long total) { totalBytes = 0; }
        
        public long getTotalBytes() { return totalBytes; }
        public long getProcessedBytes() { return totalProcessed; }
        public float getProgress() { return (float)totalProcessed/totalBytes; }
        
        public long getNumKeys() { return reduceGroups; }
        public long getNumValues() { return reduceValues; }
        
        public void nextKey() { ++reduceGroups; }
        public void nextValue() { ++reduceValues; }
        
        public void set(long bytes) { totalProcessed = bytes; }
    }
    
    private IdentityHashMap<RawKeyValueIterator,Stats> instances = new IdentityHashMap<RawKeyValueIterator,Stats>();
    private ReduceStatTracker() {}
    private static final ReduceStatTracker _instance = new ReduceStatTracker();
    
    public static Stats register(RawKeyValueIterator i,long totalBytes) {
        return _instance._register(i, totalBytes);
    }
    
    public static Stats get(RawKeyValueIterator i) {
        return _instance._get(i);
    }
    
    private synchronized Stats _register(RawKeyValueIterator i,long totalBytes) {
        Stats ret = new Stats(totalBytes);
        instances.put(i, ret);
        return ret;
    }
    
    private synchronized Stats _get(RawKeyValueIterator i) {
        return instances.get(i);
    }
}
