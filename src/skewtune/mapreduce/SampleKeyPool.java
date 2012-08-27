package skewtune.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import skewtune.mapreduce.PartitionMapOutput.LessThanKey;
import skewtune.mapreduce.SampleMapOutput.KeyPair;
import skewtune.mapreduce.SampleMapOutput.KeyPairFactory;
import skewtune.mapreduce.SampleMapOutput.SampleInterval;

public class SampleKeyPool<K> {
    private static final Log LOG = LogFactory.getLog(SampleKeyPool.class);
    
    public static final int DEFAULT_MIN_SAMPLE = 10;
    public static final int DEFAULT_MIN_INTERVAL = 8192;
    public static final int DEFAULT_MAX_INTERVAL = 1 << 20; // 1MB

    /**
     * when written directly, only records and bytes field are written
     * 
     * @author yongchul
     *
     * @param <K>
     */
    public static class KeyInfo<K> {
        K key;
        int recs;
        long bytes;
        
        public boolean isSet() { return key != null; }
        
        public K getKey() { return key; }
        public int getNumRecords() { return recs; }
        public long getTotalBytes() { return bytes; }
        
        public void set(int recs,long bytes) {
            this.recs = recs;
            this.bytes = bytes;
        }
        public void add(int v) {
            ++recs; bytes += v;
        }
        
        public void reset() {
            reset(null);
        }
        public void reset(K k) {
            key = k;
            recs = 0;
            bytes = 0;
        }
    }
    
    public static class KeyInterval<K> {
        final KeyInfo<K> begin;
        int midKeys;
        int midRecs;
        long midBytes;
        KeyInfo<K> end;
        
        KeyInterval() {
            begin = new KeyInfo<K>();
            end = new KeyInfo<K>();
        }
        
        KeyInterval(KeyInfo<K> b,int mk,int mr,long mb,KeyInfo<K> e) {
            begin = b;
            midKeys = mk;
            midRecs = mr;
            midBytes = mb;
            end = e;
        }
        
        public KeyInfo<K> getBegin() { return begin; }
        public KeyInfo<K> getEnd() { return end; }
        public int getMidKeys() { return midKeys; }
        public int getMidRecords() { return midRecs; }
        public long getMidBytes() { return midBytes; }
        
        public int getTotalRecords() { return begin.getNumRecords() + midRecs + ( end.isSet() ? end.getNumRecords() : 0 ); }
        public int getTotalKeys() { return 1 + midKeys + ( end.isSet() ? 1 : 0 ); }
        public long getTotalBytes() { return begin.getTotalBytes() + midBytes + ( end.isSet() ? end.getTotalBytes() : 0 ); }
        
        public KeyInfo<K> start(K key) {
            KeyInfo<K> current = end;
            if ( ! begin.isSet() ) {
                begin.reset(key);
                current = begin;
            } else {
                if ( end.isSet() ) {
                    ++midKeys;
                    midRecs += end.getNumRecords();
                    midBytes += end.getTotalBytes();
                }
                end.reset(key);
                current = end;
            }
            return current;
        }
        
        public void reset() {
            begin.reset();
            end.reset();
            midKeys = 0;
            midRecs = 0;
            midBytes = 0;
        }
        
        public KeyInfo<K> last() { return end.isSet() ? end : begin; }
        
        public long getLength() {
            return begin.getTotalBytes() + midBytes + end.getTotalBytes();
        }
        
        public void merge(KeyInterval<K> other) {
            midKeys += other.midKeys;
            midRecs += other.midRecs;
            midBytes += other.midBytes;
            if ( end.isSet() ) {
                ++midKeys;
                midRecs += end.getNumRecords();
                midBytes += end.getTotalBytes();
            }
            if ( other.end.isSet() ) {
                ++midKeys;
                midRecs += other.begin.getNumRecords();
                midBytes += other.begin.getTotalBytes();
                end = other.end;
            } else {
                // other.begin will be my this end
                end = other.begin;
            }
        }
    }
    
    public static interface OutputContext<K> {
        public void write(KeyInterval<K> interval) throws IOException, InterruptedException;
    }
    
    public static class LessThanKeyOutputContext<K> implements OutputContext<K> {
        final TaskInputOutputContext<?,?,K,LessThanKey> context;
        final LessThanKey rawVal = new LessThanKey();

        public LessThanKeyOutputContext(final TaskInputOutputContext<?,?,K,LessThanKey> context) {
            this.context = context;
        }

        @Override
        public void write(KeyInterval<K> interval) throws IOException, InterruptedException {
            if ( interval.getLength() == 0 ) return;
            
            // begin is not null
            rawVal.setEnd( interval.begin.getNumRecords(), interval.begin.getTotalBytes() );
            context.write( interval.begin.getKey(), rawVal );
            
            if ( LOG.isTraceEnabled() ) {
                LOG.trace(interval.begin.getKey() + " : " + rawVal);
            }
            
            if ( interval.end.isSet() ) {
                rawVal.setEnd( interval.end.getNumRecords(), interval.end.getTotalBytes() );
                rawVal.setMid( interval.midKeys, interval.midRecs, interval.midBytes );
                context.write( interval.end.getKey(), rawVal );
                
                if ( LOG.isTraceEnabled() ) {
                    LOG.trace(interval.end.getKey() + " : " + rawVal);
                }
            }
        }

    }
    
    /**
     * target interval
     */
    long targetInterval = DEFAULT_MAX_INTERVAL;
    
    /**
     * minimum sample
     */
    int minSamples = DEFAULT_MIN_SAMPLE;
    
    /**
     * current sample interval
     */
    long sampleInterval = DEFAULT_MIN_INTERVAL;
    
    List<KeyInterval<K>> buffer = new ArrayList<KeyInterval<K>>(minSamples<<1);
    long bufferedSize;
    
    KeyInterval<K> currentInterval = new KeyInterval<K>();
    KeyInfo<K> currentKeyInfo;
//    LessThanKey rawVal = new LessThanKey();
    
    public SampleKeyPool() {}
    
    public SampleKeyPool(int minSample,int interval) {
        this.minSamples = minSample;
        this.targetInterval = interval;
    }
    
    public void setSampleInterval(long sampleInterval2) {
        this.targetInterval = sampleInterval2;
    }
    
    private void compact(OutputContext<K> context) throws IOException, InterruptedException {
        sampleInterval = Math.min(sampleInterval<<1, targetInterval);
        
        LOG.info("running compaction. new sample interval = "+sampleInterval);
        
        List<KeyInterval<K>> newBuf = new ArrayList<KeyInterval<K>>(minSamples<<1);
        int i = 0;
        while ( i < buffer.size() ) {
            KeyInterval<K> current = buffer.get(i);
            newBuf.add(current);
            long remain = sampleInterval - current.getLength();
            for ( ++i ; remain > 0 && i < buffer.size(); ++i ) {
                KeyInterval<K> follow = buffer.get(i);
                long sz = follow.getLength();
                if ( remain < sz ) {
                    if ( sz - remain > remain ) {
                        // don't merge it.
                    } else {
                        current.merge(follow);
                        ++i;
                    }
                    break;
                } else {
                    current.merge(follow);
                    remain -= sz;
                }
            }
        }
        buffer = newBuf;
        currentInterval = buffer.remove(buffer.size()-1);
        LOG.info(buffer.size()+" intervals have been survived ("+bufferedSize+" bytes)");
        
        if ( sampleInterval >= targetInterval ) {
            // now it's time to flush
            for ( KeyInterval<K> x : buffer ) {
                context.write(x);
            }
            buffer.clear();
            LOG.info("switching to streamining mode.");
        }
    }
    
    public void addKeyGroup(OutputContext<K> context,K key,int nrecs,long sz) throws IOException, InterruptedException {
        if ( sampleInterval >= targetInterval ) {
            if ( sz >= sampleInterval || currentInterval.getLength() >= sampleInterval ) {
                context.write(currentInterval);
                currentInterval.reset();
            }
            currentKeyInfo = currentInterval.start(key);
            currentKeyInfo.set(nrecs, sz);
        } else {
            if ( sz >= sampleInterval || currentInterval.getLength() >= sampleInterval ) {
                // we need to buffer this
                // if currentInterval is empty, then we should skip buffering.
                if ( currentInterval.getTotalBytes() > 0 )
                    buffer.add(currentInterval);
                if ( buffer.size() >= minSamples*2 ) {
                    compact(context);
                    addKeyGroup(context,key,nrecs,sz); // retry. should make some space!
                } else {
                    currentInterval = new KeyInterval<K>();
                }
            }
            currentKeyInfo = currentInterval.start(key);
            currentKeyInfo.set(nrecs,sz);            
        }
        bufferedSize += sz;
    }
    
    public K start(OutputContext<K> context,K key,int sz) throws IOException, InterruptedException {
        if ( sampleInterval >= targetInterval ) {
            // if we are in streamining mode, only think about the current one
            if ( sz >= sampleInterval || currentInterval.getLength() >= sampleInterval ) {
                context.write(currentInterval);
                currentInterval.reset();
            }
            currentKeyInfo = currentInterval.start(key);
            currentKeyInfo.add(sz);
        } else {
            if ( sz >= sampleInterval || currentInterval.getLength() >= sampleInterval ) {
                // we need to buffer this
                if ( currentInterval.getTotalBytes() > 0 )
                    buffer.add(currentInterval);
                if ( buffer.size() >= minSamples*2 ) {
                    compact(context);
                    return start(context,key,sz); // retry. should make some space!
                } else {
                    currentInterval = new KeyInterval<K>();
                }
            }
            currentKeyInfo = currentInterval.start(key);
            currentKeyInfo.add(sz);
        }
        bufferedSize += sz;
        return currentKeyInfo.getKey();
    }
    
    public void reset(OutputContext<K> context) throws IOException, InterruptedException {
        // starting a new sequence
        if ( sampleInterval < targetInterval ) {
            // write out all buffered
            for ( KeyInterval<K> k : buffer ) {
                context.write(k);
            }
            LOG.info("total "+buffer.size()+" intervals have written");
        } else {
            context.write(currentInterval);
        }
        currentInterval.reset();
        bufferedSize = 0;
        buffer.clear();
        sampleInterval = DEFAULT_MIN_INTERVAL;
    }
    
    public void add(int sz) {
        currentKeyInfo.add(sz);
        bufferedSize += sz;
    }
    
    /*
    public void write(TaskInputOutputContext<?,?,K,LessThanKey> context,KeyInterval<K> interval) throws IOException, InterruptedException {
        if ( interval.getLength() == 0 ) return;
        
        // begin is not null
        rawVal.setEnd( interval.begin.getNumRecords(), interval.begin.getTotalBytes() );
        context.write( interval.begin.getKey(), rawVal );
        
        if ( LOG.isTraceEnabled() ) {
            LOG.trace(interval.begin.getKey() + " : " + rawVal);
        }
        
        if ( interval.end.isSet() ) {
            rawVal.setEnd( interval.end.getNumRecords(), interval.end.getTotalBytes() );
            rawVal.setMid( interval.midKeys, interval.midRecs, interval.midBytes );
            context.write( interval.end.getKey(), rawVal );
            
            if ( LOG.isTraceEnabled() ) {
                LOG.trace(interval.end.getKey() + " : " + rawVal);
            }
        }
    }
    */
}
