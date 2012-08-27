package org.apache.hadoop.mapred;

import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.TaskProgress.MapProgress;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import skewtune.mapreduce.SampleRecordPool;
import skewtune.mapreduce.PartitionMapInput.MapInputPartition;
import skewtune.mapreduce.SampleRecordPool.OutputContext;
import skewtune.mapreduce.lib.input.RecordLength;

class WrappedRecordReader<K,V> implements SplittableRecordReader<K,V>, ScannableReader<K,V> {
    private static final Log LOG = LogFactory.getLog(WrappedRecordReader.class);
    
    private final JobConf job;
    private final TaskAttemptID taskid;

    private org.apache.hadoop.mapred.RecordReader<K,V> rawIn;
    
    private Counters.Counter inputByteCounter;
    private Counters.Counter inputRecordCounter;
    private Counters.Counter procByteCounter;
    private Counters.Counter procRecordCounter;
    
    private Counters.Counter scanByteCounter;
    private Counters.Counter scanRecordCounter;
    private Counters.Counter scanMillisCounter;
    

    private TaskReporter reporter;
    private MapProgress myProgress;

    private long beforePos = -1;
    private long afterPos = -1;
    private long processingBytes;
    
    boolean closed;
    boolean stopped;
    
    K scanKey;
    V scanVal;
    
    WrappedRecordReader(JobConf job,TaskAttemptID taskid,RecordReader<K,V> raw,TaskReporter reporter,MapProgress progress) {
        this.job = job;
        this.taskid = taskid;
        rawIn = raw;
        inputRecordCounter = reporter.getCounter(TaskCounter.MAP_INPUT_RECORDS);
        inputByteCounter = reporter.getCounter(FileInputFormat.COUNTER_GROUP, FileInputFormat.BYTES_READ);
        procByteCounter = reporter.getCounter("SkewReduce","PROC_BYTES");
        procRecordCounter = reporter.getCounter("SkewReduce","PROC_RECORDS");
        
        scanByteCounter = reporter.getCounter("SkewReduce","SCAN_BYTES");
        scanRecordCounter = reporter.getCounter("SkewReduce","SCAN_RECORDS");
        scanMillisCounter = reporter.getCounter("SkewReduce","SCAN_MILLIS");
        
        this.reporter = reporter;
        this.myProgress = progress;
    }
    
    class ScanRecordTask implements Callable<StopStatus>, OutputContext {
        final StopContext context;
        final SampleRecordPool samplePool;
        final ScannableReader<K,V> scanReader;
        final DataOutputBuffer buffer;
        final long startAt;
        final RelativeOffset roff;
        int numParts;
        
        ScanRecordTask(StopContext context,ScannableReader<K,V> sreader) {
            this.context = context;
            this.scanReader = sreader;
            this.roff = sreader.getRelativeOffset();
            this.samplePool = new SampleRecordPool();
            buffer = context.getBuffer();
            startAt = System.currentTimeMillis();
        }

        @Override
        public StopStatus call() throws Exception {
            LongWritable off = new LongWritable();
            RecordLength rec = new RecordLength();
            
            if ( scanReader == this ) {
                scanKey = rawIn.createKey();
                scanVal = rawIn.createValue();
            }
            
            roff.write(buffer);
            
            scanReader.initScan();
            
            while ( scanReader.scanNextKeyValue(off, rec) ) {
                samplePool.add(this, off.get(), rec);
                scanRecordCounter.increment(1);
                scanByteCounter.increment(rec.getRecordLength());
            }
            
            scanReader.closeScan();
            
            samplePool.reset(this);
            buffer.writeInt(numParts);
            
            long now = System.currentTimeMillis();

            
            if ( LOG.isInfoEnabled() ) {
                LOG.info("relative offset = "+roff);
                LOG.info("scanned records = "+scanRecordCounter.getCounter()
                        +"; bytes = "+scanByteCounter.getCounter()
                        +"; partitions = "+numParts
                        +"; elapsed = "+((now-startAt)*0.001)+"s");
            }
            
            scanMillisCounter.increment(now - startAt);
            
            return numParts > 0 ? context.setStatus(StopStatus.STOPPED) : context.setStatus(StopStatus.CANNOT_STOP);
        }

        @Override
        public void write(MapInputPartition partition) throws IOException, InterruptedException {
            if ( partition.getLength() == 0 ) return;
            partition.write(buffer);
            if ( LOG.isDebugEnabled() ) {
                LOG.debug(partition);
            }
            ++numParts;
        }
    }

    public synchronized Future<StopStatus> stop(StopContext context) throws IOException {
        if ( closed ) {
            return StopStatus.future(StopStatus.CANNOT_STOP);
        }
        if ( stopped ) {
            throw new IllegalStateException("alraedy stopped!");
        }
        
        stopped = true;
        
        // should
        ScannableReader<K,V> scanReader;
        if ( rawIn instanceof ScannableReader ) {
            scanReader = (ScannableReader<K,V>) rawIn;
        } else {
            scanReader = this;
        }
        
        return context.execute(new ScanRecordTask(context, scanReader));
    }

    @Override
    public synchronized boolean next(K key, V value) throws IOException {
        if ( stopped ) {
            // nothing remaining.
            if ( processingBytes > 0 ) {
                procByteCounter.increment(processingBytes);
                procRecordCounter.increment(1);
                processingBytes = 0;
            }
            return false;
        }
        
        if ( myProgress != null ) {
            ((TaskProgress.MapProgress)myProgress).beginNewMap();
        }
        
        boolean ret = moveToNext(key, value);
        procByteCounter.increment(processingBytes);
        if ( processingBytes != 0 ) {
            procRecordCounter.increment(1);
        }
        
        if (ret) {
            incrCounters();
        } else {
            processingBytes = 0;
        }
        return ret;
    }

    @Override
    public K createKey() {
        return rawIn.createKey();
    }

    @Override
    public V createValue() {
        return rawIn.createValue();
    }

    @Override
    public long getPos() throws IOException {
        return rawIn.getPos();
    }

    @Override
    public float getProgress() throws IOException {
        return rawIn.getProgress();
    }
    
    @Override
    public synchronized void close() throws IOException {
        closed = true;
        rawIn.close();
    }
    
    protected void incrCounters() {
        inputRecordCounter.increment(1);
        processingBytes = afterPos - beforePos;
        inputByteCounter.increment(processingBytes);
    }

    protected synchronized boolean moveToNext(K key, V value)
            throws IOException {
        beforePos = getPos();
        boolean ret = rawIn.next(key, value);
        afterPos = getPos();
        reporter.setProgress(getProgress());
        return ret;
    }
    
    @Override
    public void initScan() throws IOException {}
    
    @Override
    public synchronized boolean scanNextKeyValue(LongWritable offset,RecordLength recLen) throws IOException {
        long pos = rawIn.getPos();
        boolean more = rawIn.next(scanKey, scanVal);
        if ( more ) {
            offset.set(pos);
            long endPos = rawIn.getPos();
            int len = (int)(endPos - pos);
            recLen.set(len>>1, len>>1, len, true);
        }
        return more;
    }
    
    @Override
    public void closeScan() throws IOException {}

    
    @Override
    public StopStatus tellAndStop(DataOutput out) throws IOException {
        if ( rawIn instanceof SplittableRecordReader ) {
            return ((SplittableRecordReader<K,V>)rawIn).tellAndStop(out);
        }
        return StopStatus.CANNOT_STOP;
    }

    @Override
    public RelativeOffset getRelativeOffset() {
        return new RelativeOffset();
    }
}
