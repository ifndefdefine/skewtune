package skewtune.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import skewtune.mapreduce.PartitionMapInput.MapInputPartition;
import skewtune.mapreduce.lib.input.RecordLength;

public class SampleRecordPool {
    private static final Log LOG = LogFactory.getLog(SampleRecordPool.class);
    
    public static final int DEFAULT_MIN_SAMPLE = 10;
    public static final int DEFAULT_MIN_INTERVAL = 4096;
    public static final int DEFAULT_MAX_INTERVAL = 1 << 20; // 1MB
    
    public static interface OutputContext {
        public void write(MapInputPartition partition) throws IOException, InterruptedException;
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
    
    List<MapInputPartition> buffer = new ArrayList<MapInputPartition>(minSamples<<1);
    long bufferedSize;
    
    MapInputPartition currentPartition = new MapInputPartition();
    boolean currentClosed; // the last tuple added to current partition is splittable
    
    public SampleRecordPool() {}
    
    public SampleRecordPool(int minSample,int interval) {
        this.minSamples = minSample;
        this.targetInterval = interval;
    }
    
    public void setSampleInterval(long sampleInterval2) {
        this.targetInterval = sampleInterval2;
    }
    
    private void compact(OutputContext context) throws IOException, InterruptedException {
        sampleInterval = Math.min(sampleInterval<<1, targetInterval);
        
        LOG.info("running compaction. new sample interval = "+sampleInterval);
        
        List<MapInputPartition> newBuf = new ArrayList<MapInputPartition>(minSamples<<1);
        int i = 0;
        while ( i < buffer.size() ) {
            MapInputPartition current = buffer.get(i);
            newBuf.add(current);
            long remain = sampleInterval - current.getLength();
            for ( ++i ; remain > 0 && i < buffer.size(); ++i ) {
                MapInputPartition follow = buffer.get(i);
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
        currentPartition = buffer.remove(buffer.size()-1);
        LOG.info(buffer.size()+" intervals have been survived ("+bufferedSize+" bytes)");
        
        if ( sampleInterval >= targetInterval ) {
            // now it's time to flush
            for ( MapInputPartition x : buffer ) {
                context.write(x);
            }
            buffer.clear();
            LOG.info("switching to streamining mode.");
        }
    }
    
    public void add(OutputContext context,long off,RecordLength rec) throws IOException, InterruptedException {
        if ( currentClosed ) {
            // current partition can be closed without this record. check better case
            if ( rec.getRecordLength() >= sampleInterval ) {
                // close current.
                if ( sampleInterval >= targetInterval ) {
                    context.write(currentPartition);
                    currentPartition.reset();
                    currentClosed = false;
                } else {
                    if ( currentPartition.getLength() > 0 )
                        buffer.add(currentPartition);
                    if ( buffer.size() >= minSamples*2 ) {
                        compact(context);
                        // now this time, the current entry hasn't been added yet
                        add(context,off,rec);
                        return;
                    } else {
                        currentPartition = new MapInputPartition();
                        currentClosed = false;
                    }
                }
            }
        } 
        
        // the last record did not close the partition. we should add this record
        currentPartition.add(off, rec);
        currentClosed = rec.isSplitable();
        if ( currentClosed ) {
            // now we can emit
            if ( sampleInterval >= targetInterval ) {
                if ( currentPartition.getLength() >= sampleInterval ) {
                    context.write(currentPartition);
                    currentPartition.reset();
                    currentClosed = false;
                }
            } else {
                if ( currentPartition.getLength() >= sampleInterval ) {
                    buffer.add(currentPartition);
                    if ( buffer.size() >= minSamples*2 ) {
                        compact(context);
                    } else {
                        currentPartition = new MapInputPartition();
                        currentClosed = false;
                    }
                    // the entry has been added already. no further retry.
                    // current closed should be remain as the same.
                }
            }
        }
    }
    
    public void reset(OutputContext context) throws IOException, InterruptedException {
        // starting a new sequence
        if ( sampleInterval < targetInterval ) {
            // write out all buffered
            for ( MapInputPartition k : buffer ) {
                context.write(k);
            }
            LOG.info("total "+buffer.size()+" intervals have written");
        } else {
            context.write(currentPartition);
        }
        currentPartition.reset();
        bufferedSize = 0;
        buffer.clear();
        sampleInterval = DEFAULT_MIN_INTERVAL;
    }
    
    public static void main(String[] args) throws Exception {
        java.util.Random rand = new java.util.Random(0);
        RecordLength rec = new RecordLength();
        SampleRecordPool samplePool = new SampleRecordPool();
        OutputContext context = new OutputContext() {
            @Override
            public void write(MapInputPartition partition) throws IOException,
                    InterruptedException {
                System.out.println(partition);
            }
        };
        
        long off = 0;
        final long limit = 8 << 20;
        while ( off < limit ) {
//            int len = rand.nextInt( (3<<20)>>>1 )+2;
          int len = rand.nextInt( 65536 )+2;
          boolean split = off+len < limit && rand.nextInt(2) == 0 ? false : true;
          System.out.println("off="+off+";len="+len+";split="+split);
            rec.set(len>>>1, len>>>1, len, split);
            samplePool.add(context, off, rec);
            off += len;
        }
        samplePool.reset(context);
        
        ExecutorService svc = Executors.newSingleThreadExecutor();
        boolean result = svc.awaitTermination(60, TimeUnit.SECONDS);
        System.err.println(result);
    }
}
