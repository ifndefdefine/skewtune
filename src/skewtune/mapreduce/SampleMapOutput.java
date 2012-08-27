package skewtune.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
//import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import skewtune.mapreduce.PartitionMapOutput.LessThanKey;
import skewtune.mapreduce.PartitionMapOutput.MapOutputPartition;
import skewtune.mapreduce.PartitionPlanner.Partition;
import skewtune.mapreduce.SampleKeyPool.OutputContext;
import skewtune.mapreduce.lib.partition.GroupComparator;
import skewtune.utils.Base64;
import skewtune.utils.LoadGen;
import skewtune.utils.Utils;
import static skewtune.utils.Itertools.*;

public class SampleMapOutput extends Configured implements SkewTuneJobConfig, Tool {
    private static final Log LOG = LogFactory.getLog(SampleMapOutput.class);
    
    public static class Map<K> extends Mapper<BytesWritable,IntWritable,BytesWritable,SampleInterval<K>> {
        private long sampleInterval;
        private boolean debug;

        // serialization/filtering
        private Class<?> keyClass;
        private Deserializer<K> keyDeserializer;
        private DataInputBuffer buffer = new DataInputBuffer();
        private byte[] minKeyBytes;
        private byte[] maxKeyBytes;
        private K minKey;
        private K maxKey;
        private RawComparator<K> rawComp;
        private boolean isRawComp;
        private Counter skipCounter;
        
        private K currentKey;
        
        private K prevKey;
        private SampleKeyPool<K> samplePool = new SampleKeyPool<K>();
        private KeyPairOutputContext<K> outputContext;
        
        private Writer dataOut;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            
            if ( conf.getBoolean(SKEWTUNE_DEBUG_GENERATE_DISKLOAD_FOR_MAP, false) ) {
                new LoadGen(conf).start();
            }
            
            sampleInterval = conf.getLong(SKEWTUNE_REPARTITION_SAMPLE_INTERVAL, 1 << 20); // by default 1MB
            if ( LOG.isInfoEnabled() ) {
                LOG.info("sample interval = "+sampleInterval+" bytes");
            }
            
            samplePool.setSampleInterval(sampleInterval);
            outputContext = new KeyPairOutputContext<K>(context);

            keyClass = context.getOutputKeyClass();
            rawComp = GroupComparator.getOriginalInstance(context.getConfiguration());
            
            // configure everything related to min key, max key and so on...
            SerializationFactory factory = new SerializationFactory(conf);
            keyDeserializer = (Deserializer<K>)factory.getDeserializer(keyClass);
            keyDeserializer.open(buffer);
            
            String keyStr = conf.get(REACTIVE_REDUCE_MIN_KEY);            
            if ( keyStr != null && keyStr.length() > 0 ) {
                minKeyBytes = Base64.decode(keyStr);
                buffer.reset(minKeyBytes,minKeyBytes.length);
                minKey = keyDeserializer.deserialize(minKey);
            }
            keyStr = conf.get(REACTIVE_REDUCE_MAX_KEY);
            if ( keyStr != null && keyStr.length() > 0 ) {
                maxKeyBytes = Base64.decode(keyStr);
                buffer.reset(maxKeyBytes,maxKeyBytes.length);
                maxKey = keyDeserializer.deserialize(maxKey);
            }
            
            isRawComp = ! (rawComp instanceof GroupComparator);
            
            skipCounter = context.getCounter("SkewTune", "SkippedMapOutputRecs");

            debug = conf.getBoolean(PartitionMapOutput.PARTITION_DEBUG,false);
            
            if ( debug ) {
                TaskID myTaskID = TaskID.forName(conf.get(JobContext.TASK_ID));
                try {
                    Path dir = FileOutputFormat.getWorkOutputPath(context);
                    Path fn = new Path(dir,String.format("data-m-%05d",myTaskID.getId()));
                    FileSystem fs = fn.getFileSystem(conf);
                    dataOut = SequenceFile.createWriter(fs, conf, fn, BytesWritable.class, IntWritable.class);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        
        @Override
        protected void map(BytesWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            // filter input based on key range
            if ( isRawComp ) {
                // don't need to deserialize
                if ( (minKeyBytes != null && rawComp.compare(key.getBytes(), 0, key.getLength(), minKeyBytes, 0, minKeyBytes.length) <= 0)
                        || (maxKeyBytes != null && rawComp.compare(key.getBytes(), 0, key.getLength(), maxKeyBytes, 0, maxKeyBytes.length) > 0) ) {
                    skipCounter.increment(1);
                    return;
                }
                buffer.reset(key.getBytes(), key.getLength() );
                currentKey = keyDeserializer.deserialize(currentKey);
            } else {
                buffer.reset(key.getBytes(), key.getLength() );
                currentKey = keyDeserializer.deserialize(currentKey);
                if ( (minKey != null && rawComp.compare(currentKey, minKey) <= 0)
                        || (maxKey != null && rawComp.compare(currentKey, maxKey) > 0) ) {
                    skipCounter.increment(1);
                    return;
                }
            }
            
            // good! we got something!
            if ( debug ) {
                dataOut.append(key, value);
            }
            
            int cmp = prevKey == null ? -1 : rawComp.compare(prevKey, currentKey);
            if ( cmp < 0 ) {
                // good. we are going forward
                prevKey = this.samplePool.start(outputContext, currentKey, value.get());
                currentKey = null;
            } else if ( cmp > 0 ) { // prev > new
                LOG.info("got a new map output. clear buffered summary.");
                samplePool.reset(outputContext);
                
                prevKey = samplePool.start(outputContext, currentKey, value.get());
                currentKey = null;
            } else {
                // otherwise, we reuse the currentKey instance
                samplePool.add( value.get() );
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
//            write(context,interval);
            samplePool.reset(outputContext); // will flush any buffered
            if ( dataOut != null ) try { dataOut.close(); } catch ( IOException ignore ) {}
        }
    }

    public static class KeyAndInterval<K> {
        final K key;
        final SampleInterval<K> interval;
        final boolean close;
        
        public KeyAndInterval(K k,SampleInterval<K> i,boolean close) {
            this.key = k;
            this.interval = i;
            this.close = close;
        }
    }
        
    public static class Reduce<K> extends Reducer<BytesWritable,SampleInterval<K>,K,LessThanKey> {
        private NullWritable nullValue = NullWritable.get();
        private Writer pfOut;
        private DataOutputBuffer buffer = new DataOutputBuffer(); // hold previous key
        public  MapOutputPartition partition = new MapOutputPartition();
        protected byte[] prevKeyBytes;
        protected Serializer<K> keySerializer;
        
        private KeyPairFactory<K> keyFactory;
        protected RawComparator<K> groupComp;
        
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            String taskIdStr = conf.get(ORIGINAL_TASK_ID_ATTR);
            TaskID taskid = TaskID.forName(taskIdStr);
            String partitionFile = PartitionMapOutput.getPartitionFileName(taskid);
            
            SerializationFactory factory = new SerializationFactory(conf);
            Class<K> keyClass = (Class<K>) context.getMapOutputKeyClass();
            
            try {
                keySerializer = factory.getSerializer(keyClass);
                keySerializer.open(buffer);
                
                keyFactory = new KeyPairFactory<K>();
                keyFactory.initialize(context);

                Path dir = FileOutputFormat.getWorkOutputPath(context);
                Path fn = new Path(dir,partitionFile);
                FileSystem fs = fn.getFileSystem(conf);
                pfOut = SequenceFile.createWriter(fs, conf, fn, MapOutputPartition.class, NullWritable.class);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            groupComp = GroupComparator.getOriginalInstance(conf);
        }

        private void write(Partition partition) throws IOException {
            if ( partition.getLength() > 0 ) {
                if ( LOG.isTraceEnabled() ) {
                    LOG.trace(partition);
                }
                pfOut.append(partition, nullValue);
            }
        }
        
        private byte[] serializeKey(K key) throws IOException {
            buffer.reset();
            keySerializer.serialize(key);
            return Arrays.copyOf(buffer.getData(), buffer.getLength());
        }
        
        protected void write(K prev, int nkeys, double nrecs, double nbytes, K current) throws IOException {
            byte[] currentKeyBytes = serializeKey(current);
            
            int nr = nrecs > 0. ? (int)Math.round(nrecs) : 0;
            long nb = nbytes > 0. ? (long)Math.round(nbytes) : 0;
            
            partition.setMinKey(prevKeyBytes);
            partition.setMaxKey(currentKeyBytes);
            partition.set(nr,nb,nkeys);
            partition.excludesMinKey();
            partition.excludesMaxKey();
            
            write(partition);
            
            prevKeyBytes = currentKeyBytes;
        }
        
        protected void write(K key, double nrecs, double nbytes) throws IOException {
            byte[] currentKeyBytes = serializeKey(key);
            
            int nr = nrecs > 0. ? (int)Math.round(nrecs) : 0;
            long nb = nbytes > 0. ? (long)Math.round(nbytes) : 0;
            
            partition.setMinKey(currentKeyBytes);
            partition.setMaxKey(currentKeyBytes);
            partition.set(nr,nb,1);
            
            write(partition);
            
            prevKeyBytes = currentKeyBytes;
        }

        
        // tracks how many keys and records are remaining open
        int openIntervals;
        int openRecs;
        long openBytes;
        int openHighKeys; // sum of all high keys
        
        int newOpenRecs;
        long newOpenBytes;
        int newOpenHighKeys;
        
        ArrayList<KeyAndInterval<K>> newOpenLowKeys = new ArrayList<KeyAndInterval<K>>(1024);
        
        SampleInterval<K> ibuffer = new SampleInterval<K>();
        
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            if ( pfOut != null ) try { pfOut.close(); } catch (IOException ignore) {}
            super.cleanup(context);
        }
        
        class MidKey implements Comparable<MidKey>{
            final double keyDensity;
            final K key;
            
            MidKey(double density,K key) {
                this.keyDensity = density;
                this.key = key;
            }
            
            public K getKey() { return key; }
            public double getDensity() { return keyDensity; }
            
            @Override
            public int compareTo(MidKey other) {
                return Double.compare(other.keyDensity,keyDensity);
            }
            
            @Override
            public String toString() {
                return "key = "+key.toString()+" density = "+keyDensity;
            }
        }
        
        private void drainUntil(PriorityQueue<MidKey> q,final K key) {
            for ( Iterator<MidKey> i = q.filter(new Predicate<MidKey>() {
                @Override
                public boolean eval(MidKey value) {
                    return groupComp.compare(value.getKey(),key) < 0;
                }
            }); i.hasNext(); i.next() ) {}
        }
        
        void alignKeys(List<SampleInterval<K>> intervals)
        throws IOException, InterruptedException {
            final KeyFunc<K,SampleInterval<K>> keyFuncBeginKey = new KeyFunc<K,SampleInterval<K>>() {
                @Override
                public K eval(SampleInterval<K> v) {
                    return v.getBeginKey();
                }
            };
            final KeyFunc<K,SampleInterval<K>> keyFuncEndKey = new KeyFunc<K,SampleInterval<K>>() {
                @Override
                public K eval(SampleInterval<K> v) {
                    return v.getEndKey();
                }
            };
            
            final java.util.Comparator<SampleInterval<K>> endKeyComp = new java.util.Comparator<SampleInterval<K>>() {
                @Override
                public int compare(SampleInterval<K> o1, SampleInterval<K> o2) {
                    return groupComp.compare(o1.getEndKey(), o2.getEndKey());
                }
            };
            
            PriorityQueue<SampleInterval<K>> mypq = new PriorityQueue<SampleInterval<K>>(endKeyComp);
            PriorityQueue<MidKey> maxKeyPq = new PriorityQueue<MidKey>();
            
            int nKeys = 0; // track the number of keys that appear in sample
            
            // first pass, scan the intervals and collects global statistics
            for ( Group<K,SampleInterval<K>> group : new GroupBy<K,SampleInterval<K>>(intervals,keyFuncBeginKey,groupComp) ) {
                final K openKey = group.group();
                for ( Group<K,SampleInterval<K>> closeGroup :
                        new GroupBy<K,SampleInterval<K>>(
                                mypq.filter(new Predicate<SampleInterval<K>>() {
                                    @Override
                                    public boolean eval(SampleInterval<K> value) {
                                        return groupComp.compare(value.getEndKey(),openKey) <= 0;
                                    }
                                }),
                                keyFuncEndKey,
                                groupComp) ) {
                    for ( SampleInterval<K> value : closeGroup ) {
                        value.openKeys = nKeys - value.openKeys - 1;
                    }
                    if ( groupComp.compare(closeGroup.group(),openKey) != 0 ) {
                        ++nKeys;
                    }
                }
                
                // open intervals now
                for ( SampleInterval<K> value : group ) {
                    if ( ! value.isSingleton() ) {
                        value.openKeys = nKeys;
                        mypq.add(value);
                    }
                }
                ++nKeys;
            }

            for ( Group<K,SampleInterval<K>> closeGroup : new GroupBy<K,SampleInterval<K>>(mypq,keyFuncEndKey,groupComp) ) {
                for ( SampleInterval<K> value : closeGroup ) {
                    value.openKeys = nKeys - value.openKeys - 1;
                }
                ++nKeys;
            }
            
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("total "+nKeys+" have been sighted in the sample");
            }
            
            // SECOND PASS
            // evenly distribute records and bytes over observed intervals
            // first pass, scan the intervals and collects global statistics
            
            double openRecords = 0;
            double openRecordsPerKey = 0;
            double openBytes = 0;
            double openBytesPerKey = 0;

            double dOpenRecs = 0.;
            double dOpenRecsPerKey = 0.;
            double dOpenBytes = 0.;
            double dOpenBytesPerKey = 0.;

            K    prevKey = null;
            for ( Group<K,SampleInterval<K>> group : new GroupBy<K,SampleInterval<K>>(intervals,keyFuncBeginKey,groupComp) ) {
                final K openKey = group.group();
                
                long openedRecords = 0;
                long openedBytes = 0;
                boolean isOpeningKey = false;

                for ( Group<K,SampleInterval<K>> closeGroup :
                        new GroupBy<K,SampleInterval<K>>(
                                mypq.filter(new Predicate<SampleInterval<K>>() {
                                    @Override
                                    public boolean eval(SampleInterval<K> value) {
                                        return groupComp.compare(value.getEndKey(),openKey) <= 0;
                                    }
                                }),
                                keyFuncEndKey,
                                groupComp) ) {
                    final K closingKey = closeGroup.group();
                    
                    // find the best key
                    drainUntil(maxKeyPq,closingKey);

                    // output (prevKey,closingKey)
                    // maxKeyPq.peek().getDensity(), openRecords, openBytes
                    if ( prevKey != null ) {
                        int mkey = (int)Math.ceil(maxKeyPq.isEmpty() ? 0 : maxKeyPq.peek().getDensity());
                        write(prevKey, mkey, openRecords, openBytes, closingKey);
                    }
                    
                    // pop out everything until closing key
                    
                    int closingRecords = 0;
                    long closingBytes = 0;
                    
                    dOpenRecs = 0.;
                    dOpenRecsPerKey = 0.;
                    dOpenBytes = 0.;
                    dOpenBytesPerKey = 0.;
                    
                    for ( SampleInterval<K> value : closeGroup ) {
                        closingRecords += value.getEndRecords();
                        closingBytes += value.getEndBytes();
                        
                        double recPerKey = value.getMidRecords() / value.getMidKeys();
                        double recs = ( value.getMidRecords() - recPerKey * value.openKeys) / (value.openKeys + 1.);
                        
                        double bytesPerKey = value.getMidBytes() / value.getMidKeys();
                        double bytes = ( value.getMidBytes() - bytesPerKey * value.openKeys) / (value.openKeys + 1.);
                        
                        dOpenRecs += recs;
                        dOpenRecsPerKey += recPerKey;
                        dOpenBytes += bytes;
                        dOpenBytesPerKey += bytesPerKey;
                    }

                    openRecords -= dOpenRecs;
                    openRecordsPerKey -= dOpenRecsPerKey;
                    openBytes -= dOpenBytes;
                    openBytesPerKey -= dOpenBytesPerKey;

                    isOpeningKey = groupComp.compare(closingKey,openKey) == 0;
                    if ( ! isOpeningKey ) {
                        // output [closingKey,closingKey]
                        write(closingKey,closingRecords+openRecordsPerKey,closingBytes+openBytesPerKey);
                        // openRecordsPerKey+closingRecords, openBytesPerKey+closingBytes
                        prevKey = closingKey;
                    } else {
                        openedRecords = closingRecords;
                        openedBytes = closingBytes;
                    }
                }
                
                // output (prevKey,openKey)
                if ( prevKey != null  && ! isOpeningKey ) {
                    drainUntil(maxKeyPq,openKey);
                    int mkey = (int)Math.ceil(maxKeyPq.isEmpty() ? 0 : maxKeyPq.peek().getDensity());
                    write(prevKey, mkey, openRecords, openBytes, openKey);
                }
                
                // open intervals now
                
                dOpenRecs = 0.;
                dOpenRecsPerKey = 0.;
                dOpenBytes = 0.;
                dOpenBytesPerKey = 0.;
               
                for ( SampleInterval<K> value : group ) {
                     openedRecords += value.getBeginRecords();
                     openedBytes += value.getBeginBytes();
                    
                    if ( ! value.isSingleton() ) {
                        mypq.add(value);
                        
                        double recPerKey = value.getMidRecords() / value.getMidKeys();
                        double recs = ( value.getMidRecords() - recPerKey * value.openKeys) / (value.openKeys + 1.);
                        
                        double bytesPerKey = value.getMidBytes() / value.getMidKeys();
                        double bytes = ( value.getMidBytes() - bytesPerKey * value.openKeys) / (value.openKeys + 1.);
                        
                        dOpenRecs += recs;
                        dOpenRecsPerKey += recPerKey;
                        dOpenBytes += bytes;
                        dOpenBytesPerKey += bytesPerKey;

                        if ( 2*value.openKeys < value.getMidKeys() ) {
                            // push only when 2*value.openKeys < value.getMidKey()
                            maxKeyPq.add(new MidKey((value.getMidKeys() - value.openKeys)/(value.openKeys+1.),value.getEndKey()));
                        }
                    }
                }
                
                // output [openKey,openKey]
                write(openKey, openedRecords + openRecordsPerKey, openedBytes + openBytesPerKey);
                // openedRecords + openRecordsPerKey, openedBytes + openBytesPerKey
                
                openRecords += dOpenRecs;
                openRecordsPerKey += dOpenRecsPerKey;
                openBytes += dOpenBytes;
                openBytesPerKey += dOpenBytesPerKey;
                
                prevKey = openKey;
            }

            for ( Group<K,SampleInterval<K>> closeGroup : new GroupBy<K,SampleInterval<K>>(mypq,keyFuncEndKey,groupComp) ) {
                final K closingKey = closeGroup.group();
                
                drainUntil(maxKeyPq,closingKey);
                // output (prevKey,closingKey)
                int mkey = (int)Math.ceil(maxKeyPq.isEmpty() ? 0 : maxKeyPq.peek().getDensity());
                write(prevKey, mkey, openRecords, openBytes, closingKey);
                
                long closingRecords = 0;
                long closingBytes = 0;
                
                dOpenRecs = 0.;
                dOpenRecsPerKey = 0.;
                dOpenBytes = 0.;
                dOpenBytesPerKey = 0.;

                for ( SampleInterval<K> value : closeGroup ) {
                    closingRecords += value.getEndRecords();
                    closingBytes += value.getEndBytes();
                    
                    double recPerKey = value.getMidRecords() / value.getMidKeys();
                    double recs = ( value.getMidRecords() - recPerKey * value.openKeys) / (value.openKeys + 1.);
                    
                    double bytesPerKey = value.getMidBytes() / value.getMidKeys();
                    double bytes = ( value.getMidBytes() - bytesPerKey * value.openKeys) / (value.openKeys + 1.);
                    
                    dOpenRecs += recs;
                    dOpenRecsPerKey += recPerKey;
                    dOpenBytes += bytes;
                    dOpenBytesPerKey += bytesPerKey;
                }
                
                openRecords -= dOpenRecs;
                openRecordsPerKey -= dOpenRecsPerKey;
                openBytes -= dOpenBytes;
                openBytesPerKey -= dOpenBytesPerKey;
                
                // output [closingKey,closingKey]
                write(closingKey,closingRecords+openRecordsPerKey,closingBytes+openBytesPerKey);

                prevKey = closingKey;
            }
        }

        @Override
        public void run(Context context)
                throws IOException, InterruptedException {
            // sorted in ascending order of begin key
            List<SampleInterval<K>> intervals = new ArrayList<SampleInterval<K>>(65536);
            
            setup(context);
            // buffer everything
            while (context.nextKey()) {
                KeyPair<K> currentKey = keyFactory.deserializeKeyPair(context.getCurrentKey());
                SampleInterval<K> newValue = new SampleInterval<K>();
                newValue.set(currentKey);
                
                for ( SampleInterval<K> v : context.getValues() ) {
                    // they are all same. aggregate
                    newValue.merge(v);
                }
                
                intervals.add(newValue);
            }
            
            alignKeys(intervals);
            
            cleanup(context);
        }
    }
    
    
    public static class KeyPair<K> {
        K begin;
        K end;
        
        public KeyPair() {}
        public KeyPair(K b,K e) {
            this.begin = b;
            this.end = e;
        }
        
        public K getBegin() { return begin; }
        public K getEnd() { return end; }
    }
        
    public static class SampleInterval<K> implements Writable {
        // cases
        // 1. only begin is set
        // 2. all fields is set
        
        K begin;
        int midKeys;
        K end;

        int brecs;
        int mrecs;
        int erecs;
        
        long bbytes;
        long mbytes;
        long ebytes;
        
        transient int openKeys;
        transient int openRecs;
        transient long openBytes;
        transient boolean closed;
        
        public SampleInterval() {}
        public SampleInterval(K bk,int br,long bb, int mk,int mr,long mb, K ek, int er, long eb) {
            begin = bk;
            brecs = br;
            bbytes = bb;
            midKeys = mk;
            mrecs = mr;
            mbytes = mb;
            end = ek;
            erecs = er;
            ebytes = eb;
        }
        
        public void reset() {
            brecs = 0;
            mrecs = 0;
            erecs = 0;
            bbytes = 0;
            mbytes = 0;
            ebytes = 0;
            openKeys = 0;
            openRecs = 0;
            openBytes = 0;
        }
        
        public boolean isSingleton() {
            return end == null;
        }

        public void set(KeyPair<K> kp) {
            begin = kp.getBegin();
            end = kp.getEnd();
        }
        
        public long getTotalBytes() {
            return bbytes + mbytes + ebytes;
        }
        public int getTotalRecords() {
            return brecs + mrecs + erecs;
        }
        
        public int getBeginRecords() {
            return brecs;
        }
        public long getBeginBytes() {
            return bbytes;
        }
        
        public int getMidKeys() { return midKeys; }
        public long getMidBytes() { return mbytes; }
        public int getMidRecords() { return mrecs; }
        
        public long getEndBytes() { return ebytes; }
        public int getEndRecords() { return erecs; }
        
        public K getBeginKey() {
            return begin;
        }

        public K getEndKey() {
            return end == null ? begin : end;
        }
        
        public void setBegin(int nr,long nb) {
            brecs = nr; bbytes = nb;
        }
        
        public void setMid(int nk,int nr,long nb) {
            midKeys = nk; mrecs = nr; mbytes = nb;
        }
        
        public void setEnd(int nr,long nb) {
            erecs = nr; ebytes = nb;
        }
        
        public void setMid() {
            midKeys = 0; mrecs = 0; mbytes = 0;
        }
        public void setEnd() { erecs = 0; ebytes = 0; }
       
        public void merge(SampleInterval<K> other) {
            brecs += other.brecs;
            bbytes += other.bbytes;
            mrecs += other.mrecs;
            mbytes += other.mbytes;
            erecs += other.erecs;
            ebytes += other.ebytes;
            
            if ( midKeys < other.midKeys ) {
                midKeys = other.midKeys;
            }
        }
        
        public void merge(KeyAndInterval<K> kai) {
            SampleInterval<K> other = kai.interval;
            if ( kai.close ) {
                // other is closing. aggregate mid/end
                if ( other.end == null ) {
                    // should aggregate only begin
                    erecs += other.brecs;
                    ebytes += other.bbytes;
                } else {
                    // otherwise, aggregate mid and end
                    mrecs += other.mrecs;
                    mbytes += other.mbytes;
                    erecs += other.erecs;
                    ebytes += other.ebytes;
                }
            } else {
                // other is opening. aggregate only begin
                erecs += other.brecs;
                ebytes += other.bbytes;
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeVInt(out, brecs);
            WritableUtils.writeVLong(out, bbytes);
            
            out.writeBoolean(end != null);
            if ( end != null ) {
                WritableUtils.writeVInt(out, midKeys);
                WritableUtils.writeVInt(out, mrecs);
                WritableUtils.writeVLong(out, mbytes);

                WritableUtils.writeVInt(out, erecs);
                WritableUtils.writeVLong(out, ebytes);
            }
        }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            brecs = WritableUtils.readVInt(in);
            bbytes = WritableUtils.readVLong(in);
            
            if ( in.readBoolean() ) {
                midKeys = WritableUtils.readVInt(in);
                mrecs = WritableUtils.readVInt(in);
                mbytes = WritableUtils.readVLong(in);
                
                erecs = WritableUtils.readVInt(in);
                ebytes = WritableUtils.readVLong(in);
            } else {
                setMid();
                setEnd();
            }
        }
        
        @Override
        public String toString() {
            return String.format("%s (%d,%d bytes) .. %d keys (%d,%d bytes) .. %s (%d,%d bytes)",begin,brecs,bbytes, midKeys, mrecs,mbytes, end, erecs, ebytes);
        }
    }
    
    /**
     * encode key pair in following format
     * 
     * <4 bytes length (K1,K2)> <content of K1> <content of K2> <4 bytes length of K1>
     * @author yongchul
     *
     * @param <K>
     */
    public static class KeyPairFactory<K> {
        Serializer<K> serializer;
        Deserializer<K> deserializer;
        Class<K> keyClass;
        DataOutputBuffer output = new DataOutputBuffer();
        DataInputBuffer input = new DataInputBuffer();
        
        public KeyPairFactory() {}
        
        @SuppressWarnings("unchecked")
        public void initialize(JobContext context) throws IOException {
            SerializationFactory factory = new SerializationFactory(context.getConfiguration());
            keyClass = (Class<K>) context.getOutputKeyClass();
            serializer = factory.getSerializer(keyClass);
            serializer.open(output);
            deserializer = factory.getDeserializer(keyClass);
            deserializer.open(input);
        }
        
        public void initialize(Configuration conf,Class<K> keyClass) throws IOException {
            SerializationFactory factory = new SerializationFactory(conf);
            this.keyClass = keyClass;
            serializer = factory.getSerializer(keyClass);
            serializer.open(output);
            deserializer = factory.getDeserializer(keyClass);
            deserializer.open(input);
        }

        
        public KeyPair<K> deserializeKeyPair(BytesWritable bytes) throws IOException {
            byte[] data = bytes.getBytes();
            int len = bytes.getLength();
            int len1 = WritableComparator.readInt(data,len-4);
            int len2 = len - len1 - 4;
            input.reset(data, 0, len1);
            K begin = deserializer.deserialize(null);
            input.reset(data, len1, len2);
            K end = len2 > 0 ? deserializer.deserialize(null) : null;
            
            return new KeyPair<K>(begin,end);
        }
        
        public void serializeKeyPair(BytesWritable dst,KeyPair<K> kp) throws IOException {
            output.reset();
            serializer.serialize(kp.getBegin());
            int len1 = output.getLength();
            if ( kp.getEnd() != null ) {
                serializer.serialize(kp.getEnd());
            }
            output.writeInt(len1);
            dst.set(output.getData(), 0, output.getLength());
        }
    }

    
    public static class KeyPairOutputContext<K> implements OutputContext<K> {
        final TaskInputOutputContext<?,?,BytesWritable,SampleInterval<K>> context;
        final KeyPairFactory<K> factory;
        final BytesWritable rawKeyPair;
        final SampleInterval<K> value;
        
        public KeyPairOutputContext(TaskInputOutputContext<?,?,BytesWritable,SampleInterval<K>> context) throws IOException {
            this.context = context;
            this.factory = new KeyPairFactory<K>();
            factory.initialize(context);
            rawKeyPair = new BytesWritable();
            value = new SampleInterval<K>();
        }

        @Override
        public void write(SampleKeyPool.KeyInterval<K> interval) throws IOException,
                InterruptedException {
            if ( interval.getLength() == 0 ) return;
            KeyPair<K> kp = new KeyPair<K>(interval.begin.getKey(),interval.end.getKey());
            factory.serializeKeyPair(rawKeyPair, kp);
            value.set(kp);
            value.setBegin( interval.begin.getNumRecords(), interval.begin.getTotalBytes() );
            if ( interval.end.isSet() ) {
                value.setMid( interval.midKeys, interval.midRecs, interval.midBytes );
                value.setEnd( interval.end.getNumRecords(), interval.end.getTotalBytes() );
            } else {
                value.setMid();
                value.setEnd();
            }
            
            context.write(rawKeyPair, value);
        }
    }

    public static class Comparator extends WritableComparator implements Configurable {
        RawComparator<?> rawComp;
        
        public Comparator() {
            super(BytesWritable.class);
        }

        @Override
        public void setConf(Configuration conf) {
            // FIXME get sort comparator
            rawComp = GroupComparator.getOriginalSortComparator(conf);
            LOG.info("SORT COMPARATOR = "+rawComp.getClass());
        }

        @Override
        public Configuration getConf() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int lenB1 = readInt(b1,s1+l1-4); // it's (4bytes length + content).
            int lenB2 = readInt(b2,s2+l2-4);
            int cmp = rawComp.compare(b1, s1+4, lenB1, b2, s2+4, lenB2);
            if ( cmp == 0 ) {
                int lenE1 = l1 - lenB1 - 8;
                int lenE2 = l2 - lenB2 - 8;
                if ( lenE1 == 0 && lenE2 == 0 ) {
                    cmp = 0;
                } else if ( lenE1 == 0 ) {
                    cmp = 1;
                } else if ( lenE2 == 0 ) {
                    cmp = -1;
                } else {
                    cmp = rawComp.compare(b2, s2+4+lenB2, lenE2, b1, s1+4+lenB1, lenE1);
                }
            }
            return cmp;
        }
    }
    
    enum Action {
        DUMP,
        CONCAT,
        TEST,
        PREPARE
    }


    @Override
    public int run(String[] args) throws Exception {
        int numReduces = 1;
        String buckets = null;
        boolean local = false;
        Action action = Action.PREPARE;
        int factor = 1;
        int sample = 65536; // 64KB
        int i = 0;
        for ( ; i < args.length; ++i ) {
            String arg = args[i];
            if ( arg.charAt(0) != '-' ) break;
            if ( arg.equals("-local") ) {
                local = true;
            } else if ( arg.equals("-factor") ) {
                factor = Integer.parseInt(args[++i]);
            } else if ( arg.equals("-sample") ) {
                sample = (int)Utils.parseSize(args[++i]);
            } else {
                try {
                    action = Enum.valueOf(Action.class, arg.substring(1).toUpperCase() );
                } catch ( IllegalArgumentException x ) {}
                System.err.println("Unknown option : "+arg);
            }
        }
        
        Path inPath = new Path(args[i++]);
        
        if ( action == Action.DUMP ) {
//            return dumpFile(local,inPath);
        } else if ( action == Action.TEST ) {
//            return test(local,inPath,buckets);
        } else if ( action == Action.CONCAT ) {
//            Path outPath = new Path(args[i++]);
//            return concat(local,inPath,outPath);
        } else {
            Path outPath = new Path(args[i++]);
            
            Cluster cluster = new Cluster(getConf());
            org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(cluster);
            
            job.getConfiguration().setInt(SKEWTUNE_REPARTITION_SAMPLE_INTERVAL,sample);
            job.getConfiguration().setInt(SKEWTUNE_REPARTITION_SAMPLE_FACTOR, factor);
            job.getConfiguration().setClass(ORIGINAL_MAP_OUTPUT_KEY_CLASS, Text.class, Writable.class);
            job.getConfiguration().set(ORIGINAL_TASK_ID_ATTR,TaskID.forName("task_201111021705_0001_r_000001").toString());
            
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);
            
            job.setMapOutputKeyClass(BytesWritable.class);
            job.setMapOutputValueClass(SampleInterval.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LessThanKey.class);
            job.setSortComparatorClass(SampleMapOutput.Comparator.class);
            
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            
            job.setNumReduceTasks(1);
            
            FileSystem fs = outPath.getFileSystem(getConf());
            if ( fs.exists(outPath) ) {
                fs.delete(outPath, true);
            }
            
            FileInputFormat.addInputPath(job, inPath);
            FileOutputFormat.setOutputPath(job, outPath);
            
            job.submit();
            
            return job.waitForCompletion(true) ? 0 : 1;
        }
        
        return -1;
    }

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new SampleMapOutput(),args);
        System.exit(rc);
    }
}
