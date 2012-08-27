package skewtune.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.TokenStorage;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.JobInProgress.MapOutputSplitInfo;
import skewtune.mapreduce.JobInProgress.ReactionContext;
import skewtune.mapreduce.PartitionPlanner.Partition;
import skewtune.mapreduce.SimpleCostModel.Estimator;
import skewtune.mapreduce.lib.input.InputSplitCache;
import skewtune.mapreduce.lib.input.MapOutputKeyInputFormat;
import skewtune.mapreduce.lib.input.MapOutputSplit;
import skewtune.mapreduce.lib.partition.GroupComparator;
import skewtune.utils.Base64;
import skewtune.utils.Utils;

/**
 * collect information related to map output for given reduce
 * 
 * TODO
 * 
 * implement a new serialization engine to avoid double encoding length information of key.
 * 
 * @author yongchul
 *
 */
public class PartitionMapOutput implements SkewTuneJobConfig {
    private static final Log LOG = LogFactory.getLog(PartitionMapOutput.class.getName());
    
    public static final String DUMP_RAW_PARTITION_DATA = "skewtune.partition.dump.rawdata";
    public static final String MAX_BYTES_PER_REDUCE = "skewtune.partition.reduce.maxbytes";
    public static final String PARTITION_SAMPLE_RATE = "skewtune.partition.sample.rate";
    public static final String SCAN_METHOD = "skewtune.partition.scan.strategy";
    public static final String PARTITION_DEBUG = "skewtune.partition.debug";
    
    public static enum ScanType {
        FULL_SCAN,
        SAMPLE_ROWS,
        SAMPLE_BYTES
    }
    
    static class ScanSpec {
        final Class<? extends Mapper> mapClass;
        final Class<? extends Reducer> reduceClass;
        final Class<? extends Object> mapOutKeyClass;
        final Class<? extends Object> mapOutValClass;
        
        ScanSpec(Class<? extends Mapper> map,Class<? extends Reducer> reduce,
                Class<? extends Object> mapOutKey,Class<? extends Object> mapOutVal) {
            this.mapClass = map;
            this.reduceClass = reduce;
            this.mapOutKeyClass = mapOutKey;
            this.mapOutValClass = mapOutVal;
        }
    }
    
    static final EnumMap<ScanType,ScanSpec> specs;
    static {
        synchronized (PartitionMapOutput.class) {
            specs = new EnumMap<ScanType,ScanSpec>(ScanType.class);
//            specs.put(ScanType.FULL_SCAN, new ScanSpec(FullScanMap.class,FullScanReduce.class,RawKey.class,KeyGroupInfo.class));
//            specs.put(ScanType.SAMPLE_ROWS, new ScanSpec(SampleRowMap.class,SampleRowReduce.class,RawKey.class,IntWritable.class));
            specs.put(ScanType.SAMPLE_BYTES, new ScanSpec(SampleBytesMap.class,SampleBytesReduce.class,RawKey.class,LessThanKey.class));
        }
    }
    
    
    public static class KeyInfo implements Comparable<BinaryComparable>, Writable {
        static final byte[] EMPTY_KEY = new byte[0];

        byte[] rawKey = EMPTY_KEY;
        int    rawKeyLen;
        int    numRecs;
        long   totBytes;

        public KeyInfo() {}
        
        public KeyInfo(MapOutputPartition p) {
            set(p);
        }
        
        public KeyInfo(RawKey key,boolean copy) {
            rawKey = ( copy ) ? key.getCopy() : key.buf;
            rawKeyLen = key.getLength();
        }
        
        public KeyInfo(BytesWritable key) {
            rawKey = Arrays.copyOf(key.getBytes(), key.getLength());
            rawKeyLen = rawKey.length;
        }
        
        private void copyKey(byte[] src,int off,int len) {
            rawKeyLen = len;
            if ( rawKey == null || rawKey.length < rawKeyLen ) {
                rawKey = new byte[rawKeyLen * 3 >>> 1];
            }
            
            System.arraycopy(src,off,rawKey,0,rawKeyLen);
        }
        
        public void reset(BytesWritable key) {
            copyKey(key.getBytes(),0,key.getLength());
            numRecs = 0;
            totBytes = 0;
        }
        
        public void reset(KeyInfo key) {
            copyKey(key.getRawKey(),0,key.getRawKeyLength());
            numRecs = 0;
            totBytes = 0;
        }
        
        public void reset(RawKey key) {
            copyKey(key.getData(),key.getOffset(),key.getLength());
            numRecs = 0;
            totBytes = 0;
        }
        
        public void reset() {
//            rawKey = null;
            rawKeyLen = 0;
            numRecs = 0;
            totBytes = 0;
        }
        
        public void reset(byte[] buf,int len) {
            copyKey(buf,0,len);
            numRecs = 0;
            totBytes = 0;
        }
        
        public void setKey(RawKey key) {
            copyKey(key.getData(),key.getOffset(),key.getLength());
        }
        
        public byte[] getRawKeyCopy() {
            byte[] r = rawKey == null ? EMPTY_KEY : Arrays.copyOf(rawKey, rawKeyLen);
            if ( r.length != rawKeyLen ) {
                throw new IllegalStateException("new length="+r.length+" expected="+rawKeyLen);
            }
            return r;
        }
        public byte[] getRawKey() { return rawKey; }
        public int getRawKeyLength() { return rawKeyLen; }
        public int getNumRecords() { return numRecs; }
        public long getTotalBytes() { return totBytes; }
        public boolean isSet() { return rawKeyLen > 0; }
        
        public void add(long bytes) {
            ++numRecs; totBytes += bytes;
        }
        
        public void add(int recs,long bytes) {
            numRecs += recs;
            totBytes += bytes;
        }
        
        public void set(MapOutputPartition p) {
            copyKey(p.minKey,0,p.minKey.length);
            numRecs = p.numRecs;
            totBytes = p.totBytes;            
        }
        
        public void merge(KeyInfo o) {
            numRecs += o.numRecs;
            totBytes += o.totBytes;
        }
        
        public void merge(KeyGroupInfo o) {
            numRecs += o.getNumRecords();
            totBytes += o.getTotalBytes();
        }
        
        @Override
        public int hashCode() {
            return Arrays.hashCode(rawKey);
        }
        
        @Override
        public boolean equals(Object o) {
            if ( o == this ) return true;
            if ( o instanceof KeyInfo ) {
                return Arrays.equals(rawKey, ((KeyInfo)o).rawKey);
            }
            return false;
        }
        
        public KeyGroupInfo getKeyGroupInfo() {
            return new KeyGroupInfo(numRecs,totBytes);
        }

        @Override
        public int compareTo(BinaryComparable o) {
            return WritableComparator.compareBytes(rawKey, 0, rawKey.length, o.getBytes(), 0, o.getLength());
        }

        @Override
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeVInt(out,rawKeyLen);
            out.write(rawKey,0,rawKeyLen);
            WritableUtils.writeVInt(out, numRecs);
            WritableUtils.writeVLong(out, totBytes);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            rawKeyLen = WritableUtils.readVInt(in);
            rawKey = ( rawKey == null || rawKey.length < rawKeyLen ) ? new byte[rawKeyLen] : rawKey;
            in.readFully(rawKey,0,rawKeyLen);
            numRecs = WritableUtils.readVInt(in);
            totBytes = WritableUtils.readVLong(in);
        }
        
        @Override
        public String toString() {
            return "key="+Utils.toHex(rawKey,0,rawKeyLen)+";recs="+numRecs+";bytes="+totBytes;
        }
    }

    
    public static class RawKey extends BinaryComparable implements WritableComparable<BinaryComparable> {
        byte[] buf;
        int off;
        int len;
        
        static final RawKey EMPTY_KEY = new RawKey(new byte[0]);
        
        public RawKey() {}
        
        public RawKey(byte[] key) {
            buf = key;
            len = buf.length;
        }
        
        public RawKey(RawKey key) {
            buf = key.getCopy();
            len = buf.length;
        }

        public byte[] getCopy() {
            return Arrays.copyOfRange(buf, off, len);
        }
        
        public void setCopy(byte[] b) {
            setCopy(b,0,b.length);
        }
        
        public void setCopy(byte[] b,int off,int len) {
            this.buf = Arrays.copyOfRange(b, off, len);
            this.off = 0;
            this.len = len;
        }
        
        public void set(byte[] b) {
            set(b,0,b.length);
        }
        
        public void set(RawKey key) {
            set(key.buf,key.off,key.len);
        }
        
        public void set(byte[] b,int off,int len) {
            this.buf = b;
            this.off = off;
            this.len = len;
        }
        
        public void set(KeyInfo key) {
            this.buf = key.getRawKey();
            this.off = 0;
            this.len = key.getRawKeyLength();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeVInt(out, len);
            out.write(buf,off,len);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            len = WritableUtils.readVInt(in);
            if ( buf == null || buf.length < len ) {
                buf = new byte[len];
            }
            off = 0;
            in.readFully(buf,0,len);
        }
        
        @Override
        public int getLength() {
            return len;
        }

        @Override
        public byte[] getBytes() {
            return ( off == 0 ) ? buf : Arrays.copyOfRange(buf,off,len);
        }
        
        public byte[] getData() {
            return buf;
        }
        
        public int getOffset() {
            return off;
        }
        
        @Override
        public String toString() {
            return Utils.toHex(buf,off,len);
        }
        
        private static final int LENGTH_BYTES = 4;
        
        /** A Comparator optimized for BytesWritable. */
        public static class Comparator extends WritableComparator implements Configurable {
            RawComparator rawComp;
            
          public Comparator() {
            super(RawKey.class);
          }

          /**
           * Compare the buffers in serialized form.
           */
          @Override
          public int compare(byte[] b1, int s1, int l1,
                             byte[] b2, int s2, int l2) {
            int n1 = WritableUtils.decodeVIntSize(b1[s1]);
            int n2 = WritableUtils.decodeVIntSize(b2[s2]);
            return rawComp.compare(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
          }

            @Override
            public Configuration getConf() {
                throw new UnsupportedOperationException(); // don't support getconf()
            }

            @Override
            public void setConf(Configuration conf) {
                // we will instantiate a raw comparator for the original key and use it for comparison
                Class<? extends RawComparator> theClass = conf.getClass(ORIGINAL_MAP_OUTPUT_KEY_COMPARATOR_CLASS, null, RawComparator.class);
                if ( theClass == null )
                    throw new IllegalArgumentException("can't find original map output key comparator class");
                rawComp = ReflectionUtils.newInstance(theClass, conf);
            }
        }
    }
    
    public static class KeyGroupInfo implements Writable {
        int    numRecs;
        long   totBytes;

        public KeyGroupInfo() {}
        public KeyGroupInfo(int n,long t) { numRecs = n; totBytes = t; }
        
        public void set(int n,long t) {
            numRecs = n; totBytes = t;
        }
        public int getNumRecords() { return numRecs; }
        public long getTotalBytes() { return totBytes; }
        
        public void add(int bytes) { ++numRecs; totBytes += bytes; }
        
        public void merge(KeyGroupInfo o) {
            numRecs += o.numRecs;
            totBytes += o.totBytes;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeVInt(out, numRecs);
            WritableUtils.writeVLong(out, totBytes);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            numRecs = WritableUtils.readVInt(in);
            totBytes = WritableUtils.readVLong(in);
        }        
    }
    
    public static class MapOutputPartition implements Partition {
        public static final byte EXCL_MINKEY = 0x01;
        public static final byte EXCL_MAXKEY = 0x02;
        public static final byte UNCERTAINTY = 0x04;

        int numKeys;
        int numRecs;
        long totBytes;
        byte[] key;    // max key
        byte[] minKey; // min key
        
        int openKeysLow;
        int openKeysHigh;
        int openRecs;
        long openBytes;
        
        byte flag;
        
        private transient int index;
        private transient double cost;
        
        public MapOutputPartition() {}
        
        public void setMinKey(KeyInfo ki) {
            this.minKey = ki.getRawKeyCopy();
        }
        public void setMaxKey(KeyInfo ki) {
            this.key = ki.getRawKeyCopy();
        }
        
        public void setMinKey(RawKey key) {
            this.minKey = key.getCopy();
        }
        public void setMaxKey(RawKey key) {
            this.key = key.getCopy();
        }
        
        public void setMinKey(byte[] key) {
            this.minKey = key;
        }
        public void setMaxKey(byte[] key) {
            this.key = key;
        }

        public byte[] getMinKey() { return minKey; }
        public byte[] getMaxKey() { return key; }
        
        public void reset() {
            numKeys = 0;
            numRecs = 0;
            totBytes = 0;
            key = minKey = null;
            flag = 0;
        }
        
        public void reset(KeyInfo ki) {
            numKeys = 1;
            numRecs = ki.getNumRecords();
            totBytes = ki.getTotalBytes();
            key = minKey = ki.getRawKeyCopy();
            flag = 0;
        }
        
        public void add(KeyInfo info) {
            ++numKeys;
            numRecs += info.getNumRecords();
            totBytes += info.getTotalBytes();
            key = info.getRawKeyCopy();
            if ( minKey == null ) minKey = key;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeVInt(out, key.length);
            out.write(key);

            WritableUtils.writeVInt(out, numRecs);
            WritableUtils.writeVLong(out, totBytes);
            WritableUtils.writeVInt(out, numKeys);
            
            out.writeByte(flag);
            
            if ( key == minKey ) {
                WritableUtils.writeVInt(out, 0); // same as the key
            } else {
                WritableUtils.writeVInt(out, minKey.length);
                out.write(minKey);
            }
            
            if ( (flag & UNCERTAINTY) != 0 ) {
                WritableUtils.writeVInt(out, openRecs);
                WritableUtils.writeVLong(out, openBytes);
                WritableUtils.writeVInt(out, openKeysLow);
                WritableUtils.writeVInt(out, openKeysHigh);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int sz = WritableUtils.readVInt(in);
            key = new byte[sz];
            in.readFully(key);
            
            numRecs = WritableUtils.readVInt(in);
            totBytes = WritableUtils.readVLong(in);
            numKeys =WritableUtils.readVInt(in);
            
            flag = in.readByte();
            
            sz = WritableUtils.readVInt(in);
            if ( sz == 0 ) {
                minKey = key;
            } else {
                minKey = new byte[sz];
                in.readFully(minKey);
            }
            
            if ( (flag & UNCERTAINTY) != 0 ) {
                openRecs = WritableUtils.readVInt(in);
                openBytes = WritableUtils.readVLong(in);
                openKeysLow = WritableUtils.readVInt(in);
                openKeysHigh = WritableUtils.readVInt(in);
            }
        }

        @Override
        public long getLength() {
            return totBytes;
        }

        public void add(KeyGroupInfo info) {
            numRecs += info.getNumRecords();
            totBytes += info.getTotalBytes();
        }
        
        public void addKey(RawKey key) {
            ++numKeys;
            this.key = key.getCopy();
            if ( minKey == null ) minKey = this.key;
        }

//        public void add(MapOutputPartition other) {
//            numRecs += other.numRecs;
//            totBytes += other.totBytes;
//            distKeysLow += other.distKeysLow;
//            distKeysHigh += other.distKeysHigh;
//            key = other.key;
//            if ( minKey == null ) minKey = this.key;
//        }
        
        public void set(int recs,long bytes,int keys) {
            numRecs = recs;
            totBytes = bytes;
            numKeys = keys;
            flag = 0;
        }
        
        public void setUncertainty(int recs,long bytes,int lkeys,int hkeys) {
            openRecs = recs;
            openBytes = bytes;
            openKeysLow = lkeys;
            openKeysHigh = hkeys;
            flag |= UNCERTAINTY;
        }
        
        public boolean isSingleton() { return flag == 0; }
        public boolean isMinKeyExcluded() { return (flag & EXCL_MINKEY ) != 0; }
        public boolean isMaxKeyExcluded() { return (flag & EXCL_MAXKEY ) != 0; }
        
        public void excludesMinKey() { flag |= EXCL_MINKEY; }
        public void excludesMaxKey() { flag |= EXCL_MAXKEY; }
        
        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append(index);
            buf.append(": key ").append( isMinKeyExcluded() ? '(' : '[').append(Utils.toHex(minKey)).append(',').append(Utils.toHex(key)).append( isMaxKeyExcluded() ? ");" : "];");
            buf.append(" numKeys = ").append(numKeys).append(";");
            buf.append(" numRecs = ").append(numRecs).append(';');
            buf.append(" bytes = ").append(totBytes).append(';');
            
            if ( (flag & UNCERTAINTY) != 0 ) {
                buf.append(" ukeys = [").append(openKeysLow).append(',').append(openKeysHigh).append("];");
                buf.append(" urecs = ").append(openRecs).append(';');
                buf.append(" ubytes= ").append(openBytes).append(';');
            }

            return buf.toString();
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public void setIndex(int i) {
            this.index = i;
        }

        public double getNumRecords() {
            return numRecs;
        }

//        public int getDistinctKeys() {
//            return distKeys;
//        }

        @Override
        public double getCost() {
            return cost;
        }

        @Override
        public void setCost(double c) {
            cost = c;
        }
    }
    
    private static List<KeyInfo> mergeList(List<KeyInfo> l1,List<KeyInfo> l2,RawComparator<?> rawComp) {
        if ( l2.isEmpty() ) return l1;
        
        ArrayList<KeyInfo> r = new ArrayList<KeyInfo>(l1.size()+l2.size());
        
        Iterator<KeyInfo> i1 = l1.iterator();
        Iterator<KeyInfo> i2 = l2.iterator();
        
        KeyInfo v1 = i1.hasNext() ? i1.next() : null;
        KeyInfo v2 = i2.hasNext() ? i2.next() : null;
        
        while ( v1 != null && v2 != null ) {
            byte[] b1 = v1.getRawKey();
            byte[] b2 = v2.getRawKey();
            int cmp = rawComp.compare(b1, 0, v1.getRawKeyLength(), b2, 0, v2.getRawKeyLength());
            if ( cmp < 0 ) {
                r.add(v1);
                v1 = i1.hasNext() ? i1.next() : null;
            } else if ( cmp > 0 ) {
                r.add(v2);
                v2 = i2.hasNext() ? i2.next() : null;
            } else { // should not happen!
                v1.merge(v2);
                r.add(v1);
                v1 = i1.hasNext() ? i1.next() : null;
                v2 = i2.hasNext() ? i2.next() : null;
            }
        }
        
        if ( v1 != null ) {
            r.add(v1);
            while ( i1.hasNext() ) {
                r.add(i1.next());
            }
        }
        
        if ( v2 != null ) {
            r.add(v2);
            while ( i2.hasNext() ) {
                r.add(i2.next());
            }
        }
        
        return r;
    }
    
    static class KeyInfoComparator implements java.util.Comparator<KeyInfo> {
        final RawComparator<?> rawComp;
        
        KeyInfoComparator(RawComparator<?> rawComp) {
            this.rawComp = rawComp;
        }

        @Override
        public int compare(KeyInfo o1, KeyInfo o2) {
            return rawComp.compare(o1.rawKey,0,o1.rawKeyLen, o2.rawKey, 0, o2.rawKeyLen);
        }
    }

    /*
    public static class FullScanMap extends Mapper<BytesWritable,IntWritable,RawKey,KeyGroupInfo> {
        @Override
        public void run(Context context) throws IOException, InterruptedException {
            final RawComparator<?> rawComp = getOriginalSortComparator(context.getConfiguration());
            final KeyInfoComparator keyComp = new KeyInfoComparator(rawComp);
            byte[] minKey = Base64.decode(context.getConfiguration().get(REACTIVE_REDUCE_MIN_KEY));

            KeyInfo info = new KeyInfo();
            info.reset(minKey,minKey.length);
            
            KeyInfo currentKey = new KeyInfo();
            
            List<KeyInfo> lold = Collections.emptyList();
            ArrayList<KeyInfo> lnew = new ArrayList<KeyInfo>();
            
            int i = 0;
            while ( context.nextKeyValue() ) {
                BytesWritable key = context.getCurrentKey();
                IntWritable value = context.getCurrentValue();
                
                // if this is first time and minKey is not set, cmo < 0
                // since there is nothing in lold, it falls through cmp < 0, the new key will become info.
                int cmp = info.rawKey.length == 0 ? -1 : rawComp.compare(info.rawKey, 0, info.rawKey.length, key.getBytes(), 0, key.getLength() );
                if ( cmp < 0 ) { // prev < new
                    for ( ; i < lold.size(); ++i ) {
                        info = lold.get(i);
                        cmp = rawComp.compare(info.rawKey, 0, info.rawKeyLen, key.getBytes(), 0, key.getLength() );
                        if ( cmp < 0 ) {
                            // probe next
                        } else {
                            if ( cmp > 0 ) {
                                info = new KeyInfo(key);
                                lnew.add(info);
                            }
                            break;
                        }
                    }
                    if ( cmp < 0 ) {
                        // if we still couldn't find it, it's something new.
                        info = new KeyInfo(key);
                        lnew.add(info);
                    }
                } else if ( cmp > 0 ) { // prev > new
                    lold = mergeList(lold,lnew,rawComp);
                    lnew.clear();
                    
                    currentKey.reset(key);
                    i = Collections.binarySearch(lold, currentKey, keyComp);
                    if ( i < 0 ) {
                        i = ~i;
                        info = new KeyInfo(key);
                        lnew.add(info);
                    } else {
                        info = lold.get(i);
                    }
                }
                
                info.add( value.get() );
            }
            
            lold = mergeList(lold,lnew,rawComp); // final merge
            
            // we are all done. write all info into map output. no need to sort.
            RawKey key = new RawKey();
            KeyGroupInfo val = new KeyGroupInfo();
            for ( KeyInfo keyInfo : lold ) {
                key.set(keyInfo.getRawKey());
                val.set(keyInfo.getNumRecords(),keyInfo.getTotalBytes());
                context.write(key, val);
            }
        }
    }
    */
    
    public static class LessThanKey implements Writable {
        boolean singleton;
        int    numRecs; // for the key
        long   totBytes;
        
        int    ltKeys;
        int    ltNumRecs;
        long   ltTotBytes;

        public LessThanKey() {}
        
        public boolean isSingleton() { return singleton; }
        
        public void setEnd(int nRecs,long nBytes) {
            singleton = true;
            numRecs = nRecs;
            totBytes = nBytes;
            ltKeys = 0;
            ltNumRecs = 0;
            ltTotBytes = 0;
        }
        
        public void setMid(int nk,int nr,long nb) {
            singleton = false;
            ltKeys = nk;
            ltNumRecs = nr;
            ltTotBytes = nb;
        }
        
        public int getNumRecords() { return numRecs; }
        public long getTotalBytes() { return totBytes; }
        
        public int getLessThanRecords() { return ltNumRecs; }
        public long getLessThanBytes() { return ltTotBytes; }
        public int getLessThanKeys() { return ltKeys; }
        
        public void add(int bytes) { ++numRecs; totBytes += bytes; }
        
        public void merge(LessThanKey o) {
            numRecs += o.numRecs;
            totBytes += o.totBytes;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeBoolean(singleton);
            WritableUtils.writeVInt(out, numRecs);
            WritableUtils.writeVLong(out, totBytes);
            if ( ! singleton ) {
                WritableUtils.writeVInt(out, ltKeys);
                WritableUtils.writeVInt(out, ltNumRecs);
                WritableUtils.writeVLong(out, ltTotBytes);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            singleton = in.readBoolean();
            numRecs = WritableUtils.readVInt(in);
            totBytes = WritableUtils.readVLong(in);
            if ( singleton ) {
                ltKeys = 0;
                ltNumRecs = 0;
                ltTotBytes = 0;
            } else {
                ltKeys = WritableUtils.readVInt(in);
                ltNumRecs = WritableUtils.readVInt(in);
                ltTotBytes = WritableUtils.readVLong(in);
            }
        }
        
        @Override
        public String toString() {
            if ( singleton ) {
                return String.format("recs=%d,bytes=%d",numRecs,totBytes);
            } else {
                return String.format("keys=%d,recs=%d,bytes=%d < recs=%d,bytes=%d", ltKeys, ltNumRecs, ltTotBytes, numRecs, totBytes);
            }
        }
    }
    
    static class KeyInterval {
        KeyInfo begin = new KeyInfo();
        // between begin and end...
        int midKeys;
        int midRecs;
        int midBytes;
        KeyInfo end = new KeyInfo();
        
        public void reset() {
            begin.reset();
            midKeys = 0;
            midRecs = 0;
            midBytes = 0;
            end.reset();
        }
        
        public KeyInfo start(BytesWritable key) {
            KeyInfo ret = end;
            if ( ! begin.isSet() ) {
                begin.reset(key);
                ret = begin;
            } else {
                if ( end.isSet() ) {
                    ++midKeys;
                    midRecs += end.getNumRecords();
                    midBytes += end.getTotalBytes();
                }
                end.reset(key);
            }
            return ret;
        }
        
        public KeyInfo start(RawKey key) {
            KeyInfo ret = end;
            if ( ! begin.isSet() ) {
                begin.reset(key);
                ret = begin;
            } else {
                if ( end.isSet() ) {
                    ++midKeys;
                    midRecs += end.getNumRecords();
                    midBytes += end.getTotalBytes();
                }
                end.reset(key);
            }
            return ret;
        }
        
        public void add(LessThanKey key) {
            midKeys += key.getLessThanKeys(); // FIXME should always take MAX?
//            midKeys = Math.max(midKeys, key.getLessThanKeys());
            midRecs += key.getLessThanRecords();
            midBytes += key.getLessThanBytes();
            
            KeyInfo currentKey = getCurrentKey();
            currentKey.add(key.getNumRecords(), key.getTotalBytes());
        }
        
        public long getLength() {
            return begin.getTotalBytes() + midBytes + end.getTotalBytes();
        }
        
        public KeyInfo getCurrentKey() {
            return end.getRawKeyLength() > 0 ? end : begin;
        }
        
        public boolean copyMidRange(MapOutputPartition part) {
            if ( midBytes == 0 ) return false;
            part.setMinKey(begin);
            part.set(midRecs, midBytes, midKeys);
            part.setMaxKey(end);
            part.excludesMinKey();
            part.excludesMaxKey();
            return true;
        }
        
        public boolean copyEnd(MapOutputPartition part) {
            if ( end.getTotalBytes() == 0 ) return false;
            part.reset(end);
            return true;
        }
        
        @Override
        public String toString() {
            return begin.toString() + "-(keys="+ midKeys+";recs="+midRecs+";bytes="+midBytes+")-"+end.toString();
        }
    }
    
    public interface MyContext<KEYIN,VALIN,KEYOUT,VALOUT> {
        public boolean nextKeyValue() throws IOException, InterruptedException;
        Configuration getConfiguration();
        public KEYIN getCurrentKey() throws IOException, InterruptedException;
        public VALIN getCurrentValue() throws IOException, InterruptedException;
        public void write(KEYOUT key,VALOUT val) throws IOException, InterruptedException;
    }
    
    public static class HadoopMapContext<KEYIN,VALIN,KEYOUT,VALOUT> implements MyContext<KEYIN,VALIN,KEYOUT,VALOUT> {
        final MapContext<KEYIN,VALIN,KEYOUT,VALOUT> context;
        
        HadoopMapContext(MapContext<KEYIN,VALIN,KEYOUT,VALOUT> context) {
            this.context = context;
        }
        
        @Override
        public Configuration getConfiguration() {
            return context.getConfiguration();
        }
        
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return context.nextKeyValue();
        }

        @Override
        public KEYIN getCurrentKey() throws IOException, InterruptedException {
            return context.getCurrentKey();
        }

        @Override
        public VALIN getCurrentValue() throws IOException, InterruptedException {
            return context.getCurrentValue();
        }

        @Override
        public void write(KEYOUT key, VALOUT val) throws IOException, InterruptedException {
            context.write(key, val);
        }
    }
    
    public static class FakeMapContext<KEYIN,VALIN,KEYOUT,VALOUT> implements MyContext<KEYIN,VALIN,KEYOUT,VALOUT> {
        Configuration conf;
        SequenceFile.Reader reader;
        KEYIN key;
        VALIN val;
        boolean more;
        
        FakeMapContext(Configuration conf,Path file) throws IOException {
            this.conf = conf;
            FileSystem fs = FileSystem.getLocal(conf);
            reader = new SequenceFile.Reader(fs, file, conf);
            more = true;
        }
        
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if ( !more ) { return false; }
            key = (KEYIN) reader.next(key);
            if ( key == null ) {
                key = null;
                val = null;
                more = false;
            } else {
                val = (VALIN) reader.getCurrentValue(val);
            }
            return more;
        }

        @Override
        public KEYIN getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public VALIN getCurrentValue() throws IOException, InterruptedException {
            return val;
        }

        @Override
        public void write(KEYOUT key, VALOUT val) throws IOException, InterruptedException {
        }

        @Override
        public Configuration getConfiguration() {
            return conf;
        }
    }

    
    public static class SampleBytesMap extends Mapper<BytesWritable,IntWritable,RawKey,LessThanKey> {
        RawKey rawKey = new RawKey();
        LessThanKey rawVal = new LessThanKey();
        
        private void write(MyContext context, KeyInterval interval) throws IOException, InterruptedException {
            if ( interval.getLength() == 0 ) return;
            
            // begin is not null
            rawKey.set( interval.begin );
            rawVal.setEnd( interval.begin.getNumRecords(), interval.begin.getTotalBytes() );
            context.write( rawKey, rawVal );
            
            if ( LOG.isDebugEnabled() ) {
                LOG.debug(rawKey + " : " + rawVal);
            }
            
            if ( interval.end.isSet() ) {
                rawKey.set( interval.end );
                rawVal.setEnd( interval.end.getNumRecords(), interval.end.getTotalBytes() );
                rawVal.setMid( interval.midKeys, interval.midRecs, interval.midBytes );
                context.write( rawKey, rawVal );
                
                if ( LOG.isDebugEnabled() ) {
                    LOG.debug(rawKey + " : " + rawVal);
                }
            }
        }
        
        private void runWithoutGroup(MyContext<BytesWritable,IntWritable,?,?> context) throws IOException, InterruptedException {
            KeyInfo info = new KeyInfo();
            info.reset(minKey,minKey.length);
            
            KeyInterval interval = new KeyInterval();
            KeyInfo current = interval.start(new BytesWritable(minKey));
            
            int i = 0;
            while ( context.nextKeyValue() ) {
                BytesWritable key = context.getCurrentKey();
                IntWritable value = context.getCurrentValue();
                
                if ( debug ) {
                    dataOut.append(key, value);
                }
                
                if ( value.get() < 0 ) {
                    System.err.println("invalid size: key="+Utils.toHex(current.getRawKeyCopy())+"; size="+value.get());
                }
                
                // if this is first time and minKey is not set, cmo < 0
                // since there is nothing in lold, it falls through cmp < 0, the new key will become info.
                
                // FIXME should work on grouping operator as well
                
                int cmp = current.isSet() ? rawComp.compare(current.getRawKey(), 0, current.getRawKeyLength(), key.getBytes(), 0, key.getLength() ) : -1;
                if ( cmp < 0 ) { // prev < new. this is a new key.
                    // good. we are going forward
                    if ( value.get() >= sampleInterval || interval.getLength() >= sampleInterval ) {
                        // this is a huge value
                        // first output current partition information
                        write(context,interval);
                        interval.reset();
                        // start a new partition with this key
                    }
                    
                    current = interval.start(key);
                } else if ( cmp > 0 ) { // prev > new
                    // we got a new sort sequence
                    
                    write(context,interval);
                    interval.reset();
                    
                    // start a new partition with this key
                    current = interval.start(key);
                }
                
                current.add( value.get() );
            }
            
            if ( interval.getLength() > 0 ) {
                // write out the last partition
                write(context,interval);
            }
        }

        /**
         * TODO make sure that the secondary comparator conforms the semantic
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        private void runWithGroup(MyContext<BytesWritable,IntWritable,?,?> context) throws IOException, InterruptedException {
            KeyInfo info = new KeyInfo();
            info.reset(minKey,minKey.length);
            
            KeyInterval interval = new KeyInterval();
            KeyInfo current = interval.start(new BytesWritable(minKey));
            
            while ( context.nextKeyValue() ) {
                BytesWritable key = context.getCurrentKey();
                IntWritable value = context.getCurrentValue();
                
                if ( debug ) {
                    dataOut.append(key, value);
                }
                
                // if this is first time and minKey is not set, cmo < 0
                // since there is nothing in lold, it falls through cmp < 0, the new key will become info.
                
                // FIXME should work on grouping operator as well
                
                int cmp = current.isSet() ? rawComp.compare(current.getRawKey(), 0, current.getRawKeyLength(), key.getBytes(), 0, key.getLength() ) : -1;
                if ( cmp < 0 ) { // prev < new. this is a new key.
                    // good. we are going forward
                    if ( value.get() >= sampleInterval || interval.getLength() >= sampleInterval ) {
                        // this is a huge value
                        // first output current partition information
                        write(context,interval);
                        interval.reset();
                        // start a new partition with this key
                    }
                    
                    current = interval.start(key);
                } else if ( cmp > 0 ) { // prev > new
                    // we got a new sort sequence
                    write(context,interval);
                    interval.reset();
                    
                    // start a new partition with this key
                    current = interval.start(key);
                }
                
                current.add( value.get() );
            }
            
            if ( interval.getLength() > 0 ) {
                // write out the last partition
                write(context,interval);
            }
        }

        public void run(MyContext context) throws IOException, InterruptedException {
//            setup(context);
            setup(context.getConfiguration());

            // FIXME we assume that if group compare exists, it honor the primary sort ordering. that is,
            // if keycomp(k1,k2) == 0 then grpcomp(k1,k2) == 0
            // if grpcomp(k1,k2) <> 0 then keycomp(k1,k2) <> 0 and the results agrees
            if ( hasGroupComp ) {
                runWithGroup(context);
            } else {
                runWithoutGroup(context);
            }
            
            cleanup(context);
        }
        
        private boolean hasGroupComp;
        private RawComparator<?> rawComp;
        private byte[] minKey;
        private long sampleInterval;
        private boolean debug;
        
        private Writer dataOut;
        
        protected void setup(Configuration conf) {
            hasGroupComp = conf.get(ORIGINAL_GROUP_COMPARATOR_CLASS) != null;
            rawComp = hasGroupComp ? getGroupComparator(conf) :getOriginalSortComparator(conf);
            minKey = Base64.decode(conf.get(REACTIVE_REDUCE_MIN_KEY));
            sampleInterval = conf.getLong(SKEWTUNE_REPARTITION_SAMPLE_INTERVAL, 1 << 20); // by default 1MB
            
            if ( LOG.isInfoEnabled() ) {
                LOG.info("min key = "+Utils.toHex(minKey));
                LOG.info("sample interval = "+sampleInterval+" bytes");
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            setup(conf);
            
            debug = conf.getBoolean(PARTITION_DEBUG,false);
            
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

        protected void cleanup(MyContext context)
                throws IOException, InterruptedException {
            if ( dataOut != null ) try { dataOut.close(); } catch ( IOException ignore ) {}
//            super.cleanup(context);
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if ( dataOut != null ) try { dataOut.close(); } catch ( IOException ignore ) {}
        }
        
        public static void main(String[] args) throws Exception {
            String confFile = args[0];
            Configuration conf = new Configuration(false);
            conf.addResource(new FileInputStream(confFile));
            FakeMapContext<BytesWritable,IntWritable,RawKey,LessThanKey> mapContext = new FakeMapContext<BytesWritable,IntWritable,RawKey,LessThanKey>(conf,new Path(args[1]));
            new SampleBytesMap().run(mapContext);
        }

        @Override
        public void run(Context context)
                throws IOException, InterruptedException {
            setup(context);

            // FIXME we assume that if group compare exists, it honor the primary sort ordering. that is,
            // if keycomp(k1,k2) == 0 then grpcomp(k1,k2) == 0
            // if grpcomp(k1,k2) <> 0 then keycomp(k1,k2) <> 0 and the results agrees
            if ( hasGroupComp ) {
                runWithGroup(new HadoopMapContext(context));
            } else {
                runWithoutGroup(new HadoopMapContext(context));
            }
            
            cleanup(context);
        }
    }

    public static class SampleBytesReduce extends Reducer<RawKey,LessThanKey,RawKey,KeyGroupInfo> {
        NullWritable nullValue = NullWritable.get();
        protected boolean hasGroupComp;
        protected RawComparator<?> rawComp;
        private Writer pfOut;
        
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            hasGroupComp = conf.get(ORIGINAL_GROUP_COMPARATOR_CLASS) != null;
            rawComp = hasGroupComp ? getGroupComparator(conf) : null;
            String taskIdStr = conf.get(ORIGINAL_TASK_ID_ATTR);
            TaskID taskid = TaskID.forName(taskIdStr);
            String partitionFile = getPartitionFileName(taskid);
            try {
                Path dir = FileOutputFormat.getWorkOutputPath(context);
                Path fn = new Path(dir,partitionFile);
                FileSystem fs = fn.getFileSystem(conf);
                pfOut = SequenceFile.createWriter(fs, conf, fn, MapOutputPartition.class, NullWritable.class);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        private void runWithGroup(Context context)
                throws IOException, InterruptedException {
            MapOutputPartition partition = new MapOutputPartition();
            KeyInterval interval = new KeyInterval();
            KeyInfo current = interval.getCurrentKey();
            KeyInfo prevKey = new KeyInfo();
            
            while ( context.nextKey() ) { // this is exactly one group.
                RawKey key = context.getCurrentKey();
                Iterable<LessThanKey> values = context.getValues();
                
                int cmp = current.isSet() ? rawComp.compare(current.getRawKey(), 0, current.getRawKeyLength(), key.getData(), key.getOffset(), key.getLength()) : -1;
                if ( cmp != 0 ) {
                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug(interval);
                    }
                    
                    partition.setMinKey(prevKey);
                    partition.setMaxKey(current);
                    partition.set(interval.midRecs, interval.midBytes, interval.midKeys);
                    partition.excludesMinKey();
                    partition.excludesMaxKey();
                    
                    write(partition);
                    
                    partition.reset();
                    
                    int nrecs = interval.begin.getNumRecords();
                    long nbytes = interval.begin.getTotalBytes();
                    if ( interval.end.isSet() ) {
                        nrecs += interval.end.getNumRecords();
                        nbytes += interval.end.getTotalBytes();
                    }
                    
                    partition.setMinKey(current);
                    partition.setMaxKey(current);
                    partition.set(nrecs, nbytes, 1);
                    
                    write(partition);
                                        
//                    // we reached the end of group
//                    if ( interval.copyMidRange(partition) ) {
//                        write(partition);
//                    }
//                    if ( interval.copyEnd(partition) ) {
//                        write(partition);
//                    }
                    
                    prevKey.reset(current);
                    interval.reset();
                    
                } // otherwise, we are in the same group.
                current = interval.start(key);
                for ( LessThanKey v : values ) {
                    interval.add(v);
                }
            }
            
            if ( LOG.isDebugEnabled() ) {
                LOG.debug(interval);
            }
            
            partition.setMinKey(prevKey);
            partition.setMaxKey(current);
            partition.set(interval.midRecs, interval.midBytes, interval.midKeys);
            partition.excludesMinKey();
            partition.excludesMaxKey();
            
            write(partition);
            
            partition.reset();
            
            partition.setMinKey(current);
            partition.setMaxKey(current);
            partition.set(current.getNumRecords(), current.getTotalBytes(), 1);
            
            write(partition);
            
//
//            // we reached the end of group
//            if ( interval.copyMidRange(partition) ) {
//                write(partition);
//            }
//            if ( interval.copyEnd(partition) ) {
//                write(partition);
//            }
        }
        
        private void write(Partition partition) throws IOException {
            if ( partition.getLength() > 0 ) {
                pfOut.append(partition, nullValue);
            }
        }
        
        private void runWithoutGroup(Context context)
                throws IOException, InterruptedException {
            MapOutputPartition current = new MapOutputPartition();
            KeyInfo prevKey = new KeyInfo();
            
            long cumKeys = 0;
            long cumRecs = 0;
            long cumBytes = 0;

            while ( context.nextKey() ) { // this is exactly one group.
                RawKey key = context.getCurrentKey();
                Iterable<LessThanKey> values = context.getValues();
                
                prevKey.reset(key);
                
                // we must track two things
                // information about keys less than this key
                // information about this key
                
                int ltKeys = 0;
                int ltRecs = 0;
                long ltBytes = 0;

                int nRecs = 0;
                long totBytes = 0;

                for ( LessThanKey v : values ) {
                    nRecs += v.getNumRecords();
                    totBytes += v.getTotalBytes();
                    
                    if ( ! v.isSingleton() ) {
                        ltKeys += v.getLessThanKeys(); // FIXME should take MAX to get lower bound?
//                        ltKeys = Math.max(ltKeys, v.getLessThanKeys());
                        ltRecs += v.getLessThanRecords();
                        ltBytes += v.getLessThanBytes();
                    }
                }
                
                current.setMinKey(prevKey);
                current.setMaxKey(key);
                current.set(ltRecs, ltBytes, ltKeys);
                current.excludesMinKey();
                current.excludesMaxKey();
                if ( current.getLength() > 0 ) {
                    write(current);
                }
                current.reset();

                current.set(nRecs, totBytes, 1);
                current.setMinKey(key);
                current.setMaxKey(key);
                if ( current.getLength() > 0 ) {
                    write(current);
                    current.reset();
                }
                
                if ( LOG.isInfoEnabled() ) {
                    LOG.info("lt keys = "+ltKeys+", lt recs = "+ltRecs+", lt bytes = "+ltBytes);
                    LOG.info(Utils.toHex(key.getData(),key.getOffset(),key.getLength())+", recs = "+nRecs+", bytes = "+totBytes);
                }
                
                cumKeys += ( ltKeys + 1 );
                cumRecs += ( ltRecs + nRecs );
                cumBytes += ( ltBytes + totBytes );
                
                if ( LOG.isInfoEnabled() ) {
                    LOG.info("cum keys = "+cumKeys+", cumRecs = "+cumRecs+", cumBytes = "+cumBytes);
                }
                
                prevKey.reset(key);
            }
        }

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            if ( hasGroupComp ) {
                runWithGroup(context);
            } else {
                runWithoutGroup(context);
            }
            cleanup(context);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            if ( pfOut != null ) try { pfOut.close(); } catch (IOException ignore) {}
            super.cleanup(context);
        }
    }
    
    /////////////////////////////////////////////////////////////////////////
    
    
    
    public static class SampleKeyMap<K> extends Mapper<BytesWritable,IntWritable,K,LessThanKey> {
        LessThanKey rawVal = new LessThanKey();
        
        private void write(Context context, KeyInterval interval) throws IOException, InterruptedException {
            if ( interval.getLength() == 0 ) return;
            
            // begin is not null
            rawVal.setEnd( interval.begin.getNumRecords(), interval.begin.getTotalBytes() );
            context.write( interval.begin.getKey(), rawVal );
            
            if ( LOG.isDebugEnabled() ) {
                LOG.debug(interval.begin.getKey() + " : " + rawVal);
            }
            
            if ( interval.end.isSet() ) {
                rawVal.setEnd( interval.end.getNumRecords(), interval.end.getTotalBytes() );
                rawVal.setMid( interval.midKeys, interval.midRecs, interval.midBytes );
                context.write( interval.end.getKey(), rawVal );
                
                if ( LOG.isDebugEnabled() ) {
                    LOG.debug(interval.end.getKey() + " : " + rawVal);
                }
            }
        }
        
        class KeyInfo {
            K key;
            int recs;
            long bytes;
            
            public boolean isSet() { return key != null; }
            
            public K getKey() { return key; }
            public int getNumRecords() { return recs; }
            public long getTotalBytes() { return bytes; }
            
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
        
        class KeyInterval {
            KeyInfo begin = new KeyInfo();
            int midKeys;
            int midRecs;
            long midBytes;
            KeyInfo end = new KeyInfo();
            
            public KeyInfo start(K key) {
                KeyInfo current = end;
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
            
            public long getLength() {
                return begin.getTotalBytes() + midBytes + end.getTotalBytes();
            }
            
        }
        
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
        
        KeyInfo currentKeyInfo;
        private KeyInterval interval = new KeyInterval();
        
        private K prevKey;
        private SampleKeyPool<K> samplePool = new SampleKeyPool<K>();
        private SampleKeyPool.LessThanKeyOutputContext<K> sampleOutputContext;
        
        private Writer dataOut;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            
            sampleInterval = conf.getLong(SKEWTUNE_REPARTITION_SAMPLE_INTERVAL, 1 << 20); // by default 1MB
            if ( LOG.isInfoEnabled() ) {
                LOG.info("sample interval = "+sampleInterval+" bytes");
            }
            
            samplePool.setSampleInterval(sampleInterval);
            sampleOutputContext = new SampleKeyPool.LessThanKeyOutputContext<K>(context);

            keyClass = context.getMapOutputKeyClass();
            rawComp = GroupComparator.getInstance(context);
            
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

            debug = conf.getBoolean(PARTITION_DEBUG,false);
            
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
        protected void cleanup(Context context) throws IOException, InterruptedException {
//            write(context,interval);
            samplePool.reset(sampleOutputContext); // will flush any buffered
            if ( dataOut != null ) try { dataOut.close(); } catch ( IOException ignore ) {}
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

            /*
            int cmp = currentKeyInfo == null ? -1 : rawComp.compare(currentKeyInfo.getKey(), currentKey);
            if ( cmp < 0 ) {
                // good. we are going forward
                if ( value.get() >= sampleInterval || interval.getLength() >= sampleInterval ) {
                    // this is a huge value
                    // first output current partition information
                    write(context,interval);
                    interval.reset();
                    // start a new partition with this key
                }
                currentKeyInfo = interval.start(currentKey);
                currentKey = null;
            } else if ( cmp > 0 ) { // prev > new
                // we got a new sort sequence
                write(context,interval);
                interval.reset();

                currentKeyInfo = interval.start(currentKey);
                currentKey = null;
            } // otherwise, we reuse the currentKey instance
            
            currentKeyInfo.add( value.get() );
            */
            
            int cmp = prevKey == null ? -1 : rawComp.compare(prevKey, currentKey);
            if ( cmp < 0 ) {
                // good. we are going forward
                prevKey = this.samplePool.start(sampleOutputContext, currentKey, value.get());
                currentKey = null;
//                if ( value.get() >= sampleInterval || interval.getLength() >= sampleInterval ) {
//                    // this is a huge value
//                    // first output current partition information
//                    write(context,interval);
//                    interval.reset();
//                    // start a new partition with this key
//                }
//                currentKeyInfo = interval.start(currentKey);
//                currentKey = null;
            } else if ( cmp > 0 ) { // prev > new
                LOG.info("got a new map output. clear buffered summary.");
                samplePool.reset(sampleOutputContext);
                
                prevKey = samplePool.start(sampleOutputContext, currentKey, value.get());
                currentKey = null;
            } else {
                // otherwise, we reuse the currentKey instance
                samplePool.add( value.get() );
            }
        }
    }

    public static class SampleKeyReduce<K> extends Reducer<K,LessThanKey,K,LessThanKey> {
        private NullWritable nullValue = NullWritable.get();
        private Writer pfOut;
        private Serializer<K> keySerializer;
        private DataOutputBuffer buffer = new DataOutputBuffer(); // hold previous key
        public  MapOutputPartition current = new MapOutputPartition();
        private int cumKeys;
        private int cumRecs;
        private long cumBytes;
        private byte[] prevKey;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            String taskIdStr = conf.get(ORIGINAL_TASK_ID_ATTR);
            TaskID taskid = TaskID.forName(taskIdStr);
            String partitionFile = getPartitionFileName(taskid);
            
            SerializationFactory factory = new SerializationFactory(conf);
            Class<?> keyClass = context.getMapOutputKeyClass();
            
            try {
                keySerializer = (Serializer<K>)factory.getSerializer(keyClass);
                keySerializer.open(buffer);

                Path dir = FileOutputFormat.getWorkOutputPath(context);
                Path fn = new Path(dir,partitionFile);
                FileSystem fs = fn.getFileSystem(conf);
                pfOut = SequenceFile.createWriter(fs, conf, fn, MapOutputPartition.class, NullWritable.class);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void write(Partition partition) throws IOException {
            if ( partition.getLength() > 0 ) {
                pfOut.append(partition, nullValue);
            }
        }
        
        private void setPreviousKey(K key) throws IOException {
            buffer.reset();
            keySerializer.serialize(key);
            prevKey = Arrays.copyOf(buffer.getData(), buffer.getLength());
        }
        
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            if ( pfOut != null ) try { pfOut.close(); } catch (IOException ignore) {}
            super.cleanup(context);
        }

        @Override
        protected void reduce(K key, Iterable<LessThanKey> values,Context context)
                throws IOException, InterruptedException {
            int ltKeys = 0;
            int ltRecs = 0;
            long ltBytes = 0;

            int nRecs = 0;
            long totBytes = 0;

            for ( LessThanKey v : values ) {
                nRecs += v.getNumRecords();
                totBytes += v.getTotalBytes();
                
                if ( ! v.isSingleton() ) {
                    ltKeys = Math.max(ltKeys, v.getLessThanKeys()); // at least this many keys should exist
//                    ltKeys += v.getLessThanKeys(); // shuold compute max
                    ltRecs += v.getLessThanRecords();
                    ltBytes += v.getLessThanBytes();
                }
            }
                        
            if ( ltBytes > 0 ) {
                if ( prevKey == null ) {
                    throw new IllegalStateException();
                }
                
                // create a copy of current key buffer
                current.setMinKey(prevKey);
                this.setPreviousKey(key); // will create a new instance
                current.setMaxKey(prevKey);
                current.set(ltRecs, ltBytes, ltKeys);
                current.excludesMinKey();
                current.excludesMaxKey();
                write(current);
                current.reset();
            } else {
                this.setPreviousKey(key);
            }
            
            current.set(nRecs, totBytes, 1);
            current.setMinKey(prevKey);
            current.setMaxKey(prevKey);
            if ( current.getLength() > 0 ) {
                write(current);
                current.reset();
            }
            
            cumKeys += ( ltKeys + 1 );
            cumRecs += ( ltRecs + nRecs );
            cumBytes += ( ltBytes + totBytes );
            
            if ( LOG.isInfoEnabled() ) {
                LOG.info("keys="+ltKeys+",recs="+ltRecs+",bytes="+ltBytes+"<"+Utils.toHex(prevKey)+":recs="+nRecs+",bytes="+totBytes);
                LOG.info("cumKeys="+cumKeys+",cumRecs="+cumRecs+",cumBytes="+cumBytes);
            }
        }
    }
    

    
    /////////////////////////////////////////////////////////////////////////
   /*
    public static class SampleRowMap extends Mapper<BytesWritable,IntWritable,RawKey,IntWritable> {
        protected float sampleRate;
        protected Random rand;
        private RawKey outKey = new RawKey();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            sampleRate = context.getConfiguration().getFloat(PARTITION_SAMPLE_RATE, 0.01f);
            rand = new Random();
        }

        @Override
        protected void map(BytesWritable key, IntWritable value, Context context)
                throws IOException, InterruptedException {
            if ( rand.nextFloat() < sampleRate ) {
                outKey.set(key.getBytes(),0,key.getLength());
                context.write(outKey, value);
            }
        }
    }
    
    public static class SampleRowReduce extends Reducer<RawKey,IntWritable,BytesWritable,KeyInfo> {
        NullWritable nullValue = NullWritable.get();
        protected boolean hasGroupComp;
        protected RawComparator<?> rawComp;
        private Writer pfOut;
        
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            hasGroupComp = conf.get(ORIGINAL_GROUP_COMPARATOR_CLASS) != null;
            rawComp = hasGroupComp ? getGroupComparator(conf) : null;
            String taskIdStr = conf.get(ORIGINAL_TASK_ID_ATTR);
            TaskID taskid = TaskID.forName(taskIdStr);
            String partitionFile = getPartitionFileName(taskid);
            try {
                Path dir = FileOutputFormat.getWorkOutputPath(context);
                Path fn = new Path(dir,partitionFile);
                FileSystem fs = fn.getFileSystem(conf);
                pfOut = SequenceFile.createWriter(fs, conf, fn, MapOutputPartition.class, NullWritable.class);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        private void runWithGroup(Context context)
                throws IOException, InterruptedException {
            
            MapOutputPartition current = new MapOutputPartition();
            current.addKey(RawKey.EMPTY_KEY);
            
            KeyInfo keyInfo = new KeyInfo();

            while ( context.nextKey() ) { // this is exactly one group.
                RawKey key = context.getCurrentKey();
                Iterable<IntWritable> values = context.getValues();
                
                int cmp = rawComp.compare(current.minKey, 0, current.minKey.length, key.getData(), key.getOffset(), key.getLength());
                if ( cmp != 0 ) {
                    write(current);
                    current.reset();
                }
                
                keyInfo.reset(key);
                for ( IntWritable v : values ) {
                    ++keyInfo.numRecs;
                    keyInfo.totBytes += v.get();
                }
                
                current.add(keyInfo);
            }
            
            if ( current.getLength() > 0 ) {
                write(current);
            }
        }
        
        private void write(Partition partition) throws IOException {
            if ( partition.getLength() > 0 ) {
                pfOut.append(partition, nullValue);
            }
        }
        
        private void runWithoutGroup(Context context)
                throws IOException, InterruptedException {
            MapOutputPartition current = new MapOutputPartition();
            KeyInfo keyInfo = new KeyInfo();

            while ( context.nextKey() ) { // this is exactly one group.
                RawKey key = context.getCurrentKey();
                Iterable<IntWritable> values = context.getValues();
                
                keyInfo.reset(key);
                for ( IntWritable v : values ) {
                    ++keyInfo.numRecs;
                    keyInfo.totBytes += v.get();
                }
                
                current.add(keyInfo);
                
                if ( current.getLength() > 0 ) {
                    write(current);
                    current.reset();
                }
            }
        }

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            if ( hasGroupComp ) {
                runWithGroup(context);
            } else {
                runWithoutGroup(context);
            }
            cleanup(context);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            if ( pfOut != null ) try { pfOut.close(); } catch (IOException ignore) {}
            super.cleanup(context);
        }
    }
    
    */
    
    public static Class<?> getMapOutputKeyClass(Configuration conf) {
        return conf.getClass(ORIGINAL_MAP_OUTPUT_KEY_CLASS, null, Object.class);  
    }
    
    public static RawComparator getOriginalSortComparator(Configuration conf) {
        // we will instantiate a raw comparator for the original key and use it for comparison
        Class<? extends RawComparator> theClass = conf.getClass(ORIGINAL_MAP_OUTPUT_KEY_COMPARATOR_CLASS, null, RawComparator.class);
        if ( theClass == null )
            throw new IllegalArgumentException("can't find original map output key comparator class");
        return ReflectionUtils.newInstance(theClass, conf);
    }
    
    public static RawComparator getOutputKeyComparator(Configuration conf) {
        Class<? extends RawComparator> theClass = conf.getClass(JobContext.KEY_COMPARATOR, null, RawComparator.class);
        if ( theClass != null )
                return ReflectionUtils.newInstance(theClass, conf);
        return WritableComparator.get(getMapOutputKeyClass(conf).asSubclass(WritableComparable.class));
    }

    public static RawComparator getGroupComparator(Configuration conf) {
        Class<? extends RawComparator> theClass = conf.getClass(ORIGINAL_GROUP_COMPARATOR_CLASS, null, RawComparator.class);
        if (theClass == null) {
          return getOutputKeyComparator(conf);
        }
        return new GroupComparator(getOriginalSortComparator(conf),ReflectionUtils.newInstance(theClass, conf));
    }
/*
    public static class FullScanReduce extends Reducer<RawKey,KeyGroupInfo,BytesWritable,KeyInfo> {
        private static final long DEFAULT_MAX_BYTES_PER_REDUCE = 4 * 1024 * 1024;
        private RawComparator<?> rawComp;
        private long maxBytesPerReduce; // 128 MB
        private boolean hasGroupComp;
        private Writer pfOut;
        private boolean dumpRawData;
        
        private NullWritable nullWritable = NullWritable.get();
        
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            hasGroupComp = conf.get(ORIGINAL_GROUP_COMPARATOR_CLASS) != null;
            dumpRawData = conf.getBoolean(DUMP_RAW_PARTITION_DATA, false);
            rawComp = hasGroupComp ? getGroupComparator(conf) : null;
            maxBytesPerReduce = conf.getLong(MAX_BYTES_PER_REDUCE, DEFAULT_MAX_BYTES_PER_REDUCE);
            String taskIdStr = conf.get(ORIGINAL_TASK_ID_ATTR);
            TaskID taskid = TaskID.forName(taskIdStr);
            String partitionFile = getPartitionFileName(taskid);
            try {
                Path dir = FileOutputFormat.getWorkOutputPath(context);
                Path fn = new Path(dir,partitionFile);
                FileSystem fs = fn.getFileSystem(conf);
                pfOut = SequenceFile.createWriter(fs, conf, fn, MapOutputPartition.class, NullWritable.class);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        private void write(Context context,Partition p) throws IOException, InterruptedException {
            if ( p.getLength() > 0 ) {
                pfOut.append(p, nullWritable);
            }
        }
        
        private void runWithoutGroup(Context context) throws IOException, InterruptedException {
            KeyInfo current = new KeyInfo();
            MapOutputPartition partition = new MapOutputPartition();
            BytesWritable bkey = new BytesWritable();
            
            while ( context.nextKey() ) { // this is exactly one group.
                RawKey key = context.getCurrentKey();
                Iterable<KeyGroupInfo> values = context.getValues();
                
                current.reset();
                current.setKey(key);

                for ( KeyGroupInfo info : values ) {
                    current.merge(info);
                }
                
                if ( dumpRawData ) {
                    bkey.set(key.getBytes(), 0, key.getLength());
                    context.write(bkey, current);
                }
                
                if ( current.getTotalBytes() >= maxBytesPerReduce ) {
                    // output partition
                    write(context,partition);
                    partition.reset();
                    
                    // output current
                    partition.add(current);
                    write(context,partition);
                    partition.reset();
                } else {
                    partition.add(current);
                    if ( partition.getLength() >= maxBytesPerReduce ) {
                        // output partition
                        write(context,partition);
                        partition.reset();
                    }
                }
            }
            
            if ( partition.getLength() > 0 ) {
                write(context,partition);
            }
        }
        
        private void runWithGroup(Context context) throws IOException, InterruptedException {
            BytesWritable bkey = new BytesWritable();
            KeyInfo keyInfo = new KeyInfo();
            MapOutputPartition current = new MapOutputPartition();
            MapOutputPartition partition = new MapOutputPartition();
            
            current.addKey(RawKey.EMPTY_KEY);
            
            while ( context.nextKey() ) { // this is exactly one group.
                RawKey key = context.getCurrentKey();
                Iterable<KeyGroupInfo> values = context.getValues();
                
                int cmp = rawComp.compare(current.minKey, 0, current.minKey.length, key.getData(), key.getOffset(), key.getLength());
                if ( cmp != 0 ) {
                    // starting a new group
                    if ( current.getLength() >= maxBytesPerReduce ) {
                        // output partition
                        write(context,partition);
                        partition.reset();
                        
                        // output current
                        partition.add(current);
                        write(context,partition);
                        partition.reset();
                    } else if ( current.key.length > 0 ){
                        partition.add(current);
                        if ( partition.getLength() >= maxBytesPerReduce ) {
                            // output partition
                            write(context,partition);
                            partition.reset();
                        }
                    }
                    
                    if ( dumpRawData ) {
                        // before we throw out the current partition, dump it
                        bkey.set(current.key, 0, current.key.length);
                        keyInfo.set(current);
                        context.write(bkey,keyInfo);
                    }
                    
                    current.reset();
                }

                for ( KeyGroupInfo info : values ) {
                    current.add(info);
                }
                
                // FIXME should compare against previous key.
                current.addKey(key);
            }
            
            if ( current.getLength() > 0 ) {
                partition.add(current);
                write(context,partition);
                
                if ( dumpRawData ) {
                    bkey.set(current.key, 0, current.key.length);
                    keyInfo.set(current);
                    context.write(bkey,keyInfo);
                }
            }
        }
        
        @Override
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            
            if ( hasGroupComp ) {
                runWithGroup(context);
            } else {
                runWithoutGroup(context);
            }
            
            cleanup(context);
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if ( pfOut != null ) {
                try { pfOut.close(); } catch ( IOException ignore ) {}
            }
            super.cleanup(context);
        }
    }
    */
/*
    public static class SampleReduce extends Reducer<RawKey,LessThanKey,MapOutputPartition,NullWritable> {
        private static final long DEFAULT_MAX_BYTES_PER_REDUCE = 4 * 1024 * 1024;
        private RawComparator<?> rawComp;
        private long maxBytesPerReduce; // 128 MB
        private boolean hasGroupComp;
        private Writer pfOut;
        private boolean dumpRawData;
        
        private NullWritable nullWritable = NullWritable.get();
        
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            hasGroupComp = conf.get(ORIGINAL_GROUP_COMPARATOR_CLASS) != null;
            dumpRawData = conf.getBoolean(DUMP_RAW_PARTITION_DATA, false);
            rawComp = hasGroupComp ? getGroupComparator(conf) : null;
            maxBytesPerReduce = conf.getLong(MAX_BYTES_PER_REDUCE, DEFAULT_MAX_BYTES_PER_REDUCE);
            String taskIdStr = conf.get(ORIGINAL_TASK_ID_ATTR);
            TaskID taskid = TaskID.forName(taskIdStr);
            String partitionFile = getPartitionFileName(taskid);
            try {
                Path dir = FileOutputFormat.getWorkOutputPath(context);
                Path fn = new Path(dir,partitionFile);
                FileSystem fs = fn.getFileSystem(conf);
                pfOut = SequenceFile.createWriter(fs, conf, fn, MapOutputPartition.class, NullWritable.class);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        private void write(Context context,Partition p) throws IOException, InterruptedException {
            if ( p.getLength() > 0 ) {
                pfOut.append(p, nullWritable);
            }
        }
        
        private void runWithoutGroup(Context context) throws IOException, InterruptedException {
            KeyInfo current = new KeyInfo();
            MapOutputPartition partition = new MapOutputPartition();
            BytesWritable bkey = new BytesWritable();
            
            while ( context.nextKey() ) { // this is exactly one group.
                RawKey key = context.getCurrentKey();
                Iterable<LessThanKey> values = context.getValues();
                
                current.reset();
                current.setKey(key);

                for ( LessThanKey info : values ) {
                    current.merge(info);
                }
                
                if ( dumpRawData ) {
                    bkey.set(key.getBytes(), 0, key.getLength());
                    context.write(bkey, current);
                }
                
                if ( current.getTotalBytes() >= maxBytesPerReduce ) {
                    // output partition
                    write(context,partition);
                    partition.reset();
                    
                    // output current
                    partition.add(current);
                    write(context,partition);
                    partition.reset();
                } else {
                    partition.add(current);
                    if ( partition.getLength() >= maxBytesPerReduce ) {
                        // output partition
                        write(context,partition);
                        partition.reset();
                    }
                }
            }
            
            if ( partition.getLength() > 0 ) {
                write(context,partition);
            }
        }
        
        private void runWithGroup(Context context) throws IOException, InterruptedException {
            BytesWritable bkey = new BytesWritable();
            KeyInfo keyInfo = new KeyInfo();
            MapOutputPartition current = new MapOutputPartition();
            MapOutputPartition partition = new MapOutputPartition();
            
            current.addKey(RawKey.EMPTY_KEY);
            
            while ( context.nextKey() ) { // this is exactly one group.
                RawKey key = context.getCurrentKey();
                Iterable<KeyGroupInfo> values = context.getValues();
                
                int cmp = rawComp.compare(current.minKey, 0, current.minKey.length, key.getData(), key.getOffset(), key.getLength());
                if ( cmp != 0 ) {
                    // starting a new group
                    if ( current.getLength() >= maxBytesPerReduce ) {
                        // output partition
                        write(context,partition);
                        partition.reset();
                        
                        // output current
                        partition.add(current);
                        write(context,partition);
                        partition.reset();
                    } else if ( current.key.length > 0 ){
                        partition.add(current);
                        if ( partition.getLength() >= maxBytesPerReduce ) {
                            // output partition
                            write(context,partition);
                            partition.reset();
                        }
                    }
                    
                    if ( dumpRawData ) {
                        // before we throw out the current partition, dump it
                        bkey.set(current.key, 0, current.key.length);
                        keyInfo.set(current);
                        context.write(bkey,keyInfo);
                    }
                    
                    current.reset();
                }

                for ( KeyGroupInfo info : values ) {
                    current.add(info);
                }
                
                // FIXME should compare against previous key.
                current.addKey(key);
            }
            
            if ( current.getLength() > 0 ) {
                partition.add(current);
                write(context,partition);
                
                if ( dumpRawData ) {
                    bkey.set(current.key, 0, current.key.length);
                    keyInfo.set(current);
                    context.write(bkey,keyInfo);
                }
            }
        }
        
        @Override
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            
            // from the map, we already have sampled from sorted map outputs
            // the part keys are sorted in ascending order of key range (ORDER BY min ASC, max DESC) so that we can spread.
            // in this reduce, we simply scan in order and merge or split the partition
            
            // [K1                        K2]
            //       [K3         K4]
            //    [K5 K3]   [K6      K7]
            // [K1 K5 K3     K6  K4  K7   K2]
            //
            // distribute # of records or bytes

            if ( hasGroupComp ) {
                runWithGroup(context);
            } else {
                runWithoutGroup(context);
            }
            
            cleanup(context);
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if ( pfOut != null ) {
                try { pfOut.close(); } catch ( IOException ignore ) {}
            }
            super.cleanup(context);
        }
    }
    */

    
    public static String getPartitionFileName(TaskID taskid) {
        return String.format("partition-r-%05d", taskid.getId());
    }
    
    public static Path getOutputPath(JobInProgress jip,TaskID taskid) {
        Path orgOutDir = jip.getOriginalJob().getOutputPath();
        Path outDir = new Path(orgOutDir, String.format("scan-%s-%05d",taskid.getTaskType() == TaskType.MAP ? "m" : "r", taskid.getId()));
        return outDir;
    }
    
    public static Path getPartitionFile(JobInProgress jip,TaskID taskid) {
        Path outPath = getOutputPath(jip,taskid);
        return new Path(outPath,getPartitionFileName(taskid));
    }
    
    private String getMapOutputKeyClassName(Configuration conf) {
        return conf.get(JobContext.MAP_OUTPUT_KEY_CLASS, conf.get(JobContext.OUTPUT_KEY_CLASS));
    }
    
    public org.apache.hadoop.mapreduce.Job prepareJob(JobInProgress jip,org.apache.hadoop.mapred.TaskID taskid,ReactionContext action, int maxSlots) throws IOException, InterruptedException {
        // prepare a job from given configuration
        org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(jip.getCluster());
        Configuration oldConf = jip.getConfiguration();
        Configuration newConf = job.getConfiguration();

        // setup token storage
        TokenStorage oldTokenStorage = jip.getTokenStorage(true);
        Token<JobTokenIdentifier> jt = (Token<JobTokenIdentifier>) TokenCache.getJobToken(oldTokenStorage);
        TokenStorage newTokenStorage = new TokenStorage();
        newTokenStorage.addToken(new Text(taskid.getJobID().toString()), jt);

        job.setTokenStorage(newTokenStorage);
        
        MapOutputSplitInfo splitInfo = jip.getMapOutputSplits(taskid);
        InputSplitCache.set(newConf, splitInfo.splits);
        
//        if ( LOG.isDebugEnabled() ) {
//            try {
//                LOG.debug("job file="+jip.getJob().getJobFile());
//                LOG.debug("map class="+jip.getJob().getMapperClass());
//                LOG.debug("reduce class="+jip.getJob().getReducerClass());
//                LOG.debug("map out key class="+jip.getJob().getMapOutputKeyClass());
//                LOG.debug("map out cmp class="+jip.getJob().getSortComparator());
//                LOG.debug("job out key class="+jip.getJob().getOutputKeyClass());
//            } catch ( Exception ignore ) {}
//        }
        
        newConf.set(ORIGINAL_JOB_ID_ATTR, taskid.getJobID().toString());
        newConf.set(ORIGINAL_TASK_ID_ATTR, taskid.toString());

/*
        String groupComp = oldConf.get(JobContext.GROUP_COMPARATOR_CLASS);
        if ( groupComp != null ) {
            newConf.set(ORIGINAL_GROUP_COMPARATOR_CLASS, groupComp);
        }

        newConf.setClass(ORIGINAL_MAP_OUTPUT_KEY_CLASS, jip.getConfTemplate().getMapOutputKeyClass(), Object.class);

        RawComparator<?> comp = jip.getConfTemplate().getOutputKeyComparator();
        newConf.setClass(ORIGINAL_MAP_OUTPUT_KEY_COMPARATOR_CLASS, comp.getClass(), RawComparator.class);
        job.setSortComparatorClass(RawKey.Comparator.class); // this ensures that we sort the keys in correct order
        
        job.setOutputKeyClass(RawKey.class);
        job.setOutputValueClass(KeyGroupInfo.class);
        
//        newConf.set(JobContext.COMBINE_CLASS_ATTR, null); // we don't need a combiner
        
        
        ScanType scanType = jip.getConfTemplate().getEnum(SCAN_METHOD, ScanType.SAMPLE_BYTES);
        ScanSpec scanSpec = this.specs.get(scanType);
     
//        job.setMapperClass(scanSpec.mapClass);
//        job.setReducerClass(scanSpec.reduceClass);
//        job.setMapOutputKeyClass(scanSpec.mapOutKeyClass);
//        job.setMapOutputValueClass(scanSpec.mapOutValClass);
 */

        job.setJobName("Scan-"+taskid.toString());

        // XXX
        /*
        job.setMapperClass(SampleKeyMap.class);
        job.setReducerClass(SampleKeyReduce.class);
        */
        
        job.setMapperClass(SampleMapOutput.Map.class);
        job.setReducerClass(SampleMapOutput.Reduce.class);
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(SampleMapOutput.SampleInterval.class);
        job.setSortComparatorClass(SampleMapOutput.Comparator.class);
        
        // must set original sort comparator and group comparator if exists
        // if they either of them does not exist, it must be default of the original output key
        Utils.copyConfigurationAs(oldConf, MRJobConfig.KEY_COMPARATOR, newConf, SkewTuneJobConfig.ORIGINAL_MAP_OUTPUT_KEY_COMPARATOR_CLASS);
        Utils.copyConfigurationAs(oldConf, MRJobConfig.GROUP_COMPARATOR_CLASS, newConf, SkewTuneJobConfig.ORIGINAL_GROUP_COMPARATOR_CLASS);
        
        // following may fail since the tracker do not have user class definition
//        job.setOutputKeyClass(jip.getConfTemplate().getMapOutputKeyClass());
        String orgMapOutKeyClsName = this.getMapOutputKeyClassName(jip.getConfTemplate());
        newConf.set(JobContext.OUTPUT_KEY_CLASS, orgMapOutKeyClsName );
        newConf.set(SkewTuneJobConfig.ORIGINAL_MAP_OUTPUT_KEY_CLASS, orgMapOutKeyClsName);
        
        job.setOutputValueClass(LessThanKey.class);
        
        
        job.setNumReduceTasks(1);
        
        job.setPriority(JobPriority.VERY_HIGH);
        
        job.setInputFormatClass(MapOutputKeyInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, getOutputPath(jip,taskid));
        
        newConf.set("mapreduce.job.parent",taskid.getJobID().toString());
        newConf.setBoolean("mapreduce.job.parent.sticky", true);
        
        // determine sample interval.
        // max( total bytes / (# of nodes * 10), 1)
        int factor = oldConf.getInt(SKEWTUNE_REPARTITION_SAMPLE_FACTOR, 1);
        long sampleInterval = Math.max( action.getRemainBytes() / (Math.max( splitInfo.numMapOutput, maxSlots )*10*factor), 1);
        if ( LOG.isInfoEnabled() ) {
            LOG.info(taskid+" sampling interval = "+sampleInterval);
        }
        newConf.setLong(SKEWTUNE_REPARTITION_SAMPLE_INTERVAL, sampleInterval);
        
        Utils.copyConfiguration(oldConf,newConf,MRJobConfig.CACHE_ARCHIVES);
        Utils.copyConfiguration(oldConf,newConf,MRJobConfig.CACHE_FILES);
        Utils.copyConfiguration(oldConf,newConf,MRJobConfig.CLASSPATH_ARCHIVES);
        Utils.copyConfiguration(oldConf,newConf,MRJobConfig.CLASSPATH_FILES);
        Utils.copyConfiguration(oldConf,newConf,MRJobConfig.CACHE_SYMLINK);
        Utils.copyConfiguration(oldConf,newConf,MRJobConfig.CACHE_FILE_TIMESTAMPS);
        Utils.copyConfiguration(oldConf,newConf,MRJobConfig.CACHE_ARCHIVES_TIMESTAMPS);
        Utils.copyConfiguration(oldConf,newConf,MRJobConfig.END_NOTIFICATION_URL);

        // XXX
        /*
        copyConfiguration(oldConf,newConf,MRJobConfig.KEY_COMPARATOR);
        copyConfiguration(oldConf,newConf,MRJobConfig.GROUP_COMPARATOR_CLASS);
        */

        Utils.copyConfiguration(oldConf,newConf,PARTITION_DEBUG);
        
        action.initScanTask(job);
        
        return job;
    }
    
    public static List<Partition> loadPartitionFile(FileSystem fs,Path file,Configuration conf) throws IOException {
        List<Partition> result = new ArrayList<Partition>();
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, file, conf);
            MapOutputPartition p = new MapOutputPartition();
            int i = 0;
            while ( reader.next(p) ) {
                result.add(p);
                p.setIndex(i++);
                p = new MapOutputPartition();
            }
        } finally {
            if ( reader != null ) {
                reader.close();
            }
        }
        return result;
    }
    
    public static void main(String[] args) throws Exception {
        boolean isLocal = "-l".equals(args[0]) || "-local".equals(args[0]);
        Path file = new Path(isLocal ? args[1] : args[0]);
        
        Configuration conf = new Configuration();
        FileSystem fs = isLocal ? FileSystem.getLocal(conf) : file.getFileSystem(conf);
        
        List<Partition> parts = loadPartitionFile(fs,file,conf);
        System.out.println(parts.size() + " partitions loaded");
        for ( Partition p : parts ) {
            System.out.println(p);
        }
    }
}
