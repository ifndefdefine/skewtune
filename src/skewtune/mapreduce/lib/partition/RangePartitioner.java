package skewtune.mapreduce.lib.partition;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.utils.Utils;

/**
 * The partitioner presents an illusion to delegating partitioner that there are X partitions rather than Y.
 * 
 * @author yongchul
 *
 * @param <K> Map output key type
 * @param <V> Map output value type
 */
public class RangePartitioner<K,V>
extends SkewTunePartitioner<K,V> {
    private static final Log LOG = LogFactory.getLog(RangePartitioner.class);

    List<K> buckets;
    int lastIndex;
    RawComparator<K> comp;
    
    int[] mapping;
    BitSet excludeMask;
    
    int[] order;
    
    // copied from JobConf
    
    private Class<?> getMapOutputKeyClass(Configuration conf) {
        Class<?> retv = conf.getClass(JobContext.MAP_OUTPUT_KEY_CLASS, null, Object.class);
        if (retv == null) {
          retv = conf.getClass(JobContext.OUTPUT_KEY_CLASS,LongWritable.class, Object.class);
        }
        return retv;
    }
    
    private RawComparator getOutputKeyComparator(Configuration conf) {
        Class<? extends RawComparator> theClass = conf.getClass(JobContext.KEY_COMPARATOR, null, RawComparator.class);
        if (theClass != null)
          return ReflectionUtils.newInstance(theClass, conf);
        return WritableComparator.get(getMapOutputKeyClass(conf).asSubclass(WritableComparable.class));
    }
    
    private RawComparator<K> getOutputValueGroupingComparator(Configuration conf) {
        Class<? extends RawComparator> theClass = conf.getClass(JobContext.GROUP_COMPARATOR_CLASS, null, RawComparator.class);
        if (theClass == null) {
            return getOutputKeyComparator(conf);
        }
        // if group comparator is specified, we override group comparator
        return new GroupComparator<K>(getOutputKeyComparator(conf),ReflectionUtils.newInstance(theClass, conf));
    }
    
    public RangePartitioner() {}
    
    public RangePartitioner(List<K> buckets,RawComparator<K> comp,int[] mapping,BitSet mask) {
        this.buckets = buckets;
        this.comp = comp;
        this.mapping = mapping;
        this.excludeMask = mask;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void setConf(Configuration conf) {
        int numReduces = conf.getInt(JobContext.NUM_REDUCES, 1); // number of reduces
        comp = this.getOutputValueGroupingComparator(conf);
        
        buckets = new ArrayList<K>(numReduces-1);
        excludeMask = new BitSet(numReduces-1);
        
        Class<K> keyClass = (Class<K>)getMapOutputKeyClass(conf);
        Deserializer<K> keyDeserializer;
        SerializationFactory serializationFactory = new SerializationFactory(conf);
        DataInputBuffer keyIn = new DataInputBuffer();

        DataInputStream input = null;
        try {
            input = getBucketInfo(conf);
            
            keyDeserializer = serializationFactory.getDeserializer(keyClass);
            keyDeserializer.open(keyIn);
            
            byte[] buf = new byte[8192];
            for ( int i = 0; i < numReduces-1; ++i ) {
                int keyLen = WritableUtils.readVInt(input);
                if ( buf.length < keyLen ) {
                    buf = new byte[ (keyLen*3) >> 1 ];
                }
                input.readFully(buf,0,keyLen);
                keyIn.reset(buf, 0, keyLen);
                K key = keyDeserializer.deserialize(null);
                buckets.add(key);
            }
            mapping = new int[numReduces];
            for ( int i = 0; i < numReduces; ++i ) {
                mapping[i] = WritableUtils.readVInt(input);
            }
            int numExcludes = WritableUtils.readVInt(input);
            for ( int i = 0; i < numExcludes; ++i ) {
                excludeMask.set(WritableUtils.readVInt(input));
            }
            
            order = getScheduleOrder(conf);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException("failed to parse bucket information",e);
        } finally {
            if ( input != null ) try { input.close(); } catch ( IOException ignore ) {}
        }
        
        if ( LOG.isDebugEnabled() ) {
            LOG.debug("buckets = "+buckets);
            LOG.debug("mapping = "+Arrays.toString(mapping));
            LOG.debug("exclude mask = "+excludeMask);
       		LOG.debug("schedule order = "+Arrays.toString(order));
        }
    }
    
    /**
     * FIXME binary search?
     */
    @Override
    public int getPartition(K key, V value, int n) {
        // we are reading in sorted sequences
        // buckets [ 0 1 2 3 4 5 6 ]
        // mapping [0 1 2 3 4 5 6 7]
        int rc;
        if ( lastIndex > 0 ) {
            rc = comp.compare(buckets.get(lastIndex-1),key);
            if ( rc > 0 ) {
                // new map output stream has started. we fall back to binary search this time.
                int prevLastIndex = lastIndex;
                lastIndex = Collections.binarySearch(buckets, key, comp);
                lastIndex = lastIndex < 0 ? ~lastIndex : (excludeMask.get(lastIndex) ? lastIndex : lastIndex + 1);
                
//                if ( LOG.isDebugEnabled() ) {
//                    LOG.debug("key="+key+";preidx="+prevLastIndex+";newidx="+lastIndex);
//                }
                
                return order[mapping[lastIndex]];
            } else if ( rc == 0 && excludeMask.get(lastIndex-1) ) {
//                if ( LOG.isDebugEnabled() ) {
//                    LOG.debug("key="+key+";preidx="+lastIndex+";newidx="+(lastIndex-1));
//                }
                lastIndex = lastIndex - 1;
                return order[mapping[lastIndex]];
            }
        }
        
        for ( ; lastIndex < buckets.size(); ++lastIndex ) {
            // ff120000 -- ffb2c000 
            rc = comp.compare(key, buckets.get(lastIndex));
            if ( rc < 0 || (rc == 0 && excludeMask.get(lastIndex)) ) {
                return order[mapping[lastIndex]];
            }
        }
        
        return order[mapping[n-1]];
    }
    /*
    public static void main(String[] args) throws Exception {
        ArrayList<BytesWritable> buckets = new ArrayList<BytesWritable>();
        buckets.add(new BytesWritable(new byte[] { (byte)0x14, (byte)0xd7, (byte)0x40, 0x00, 0x01 }));
        buckets.add(new BytesWritable(new byte[] { (byte)0x66, (byte)0x39, (byte)0xc0, 0x00, 0x01}));
        buckets.add(new BytesWritable(new byte[] { (byte)0xa5, (byte)0x34, (byte)0x40, 0x00, 0x01 }));
        buckets.add(new BytesWritable(new byte[] { (byte)0xff, (byte)0xee, (byte)0x40, 0x00, 0x01 }));
        buckets.add(new BytesWritable(new byte[] { (byte)0xff, (byte)0xff, (byte)0xc0, 0x00, 0x01 }));
        
        BitSet excludeMask = new BitSet(buckets.size());
        excludeMask.set(1);
        excludeMask.set(3);
        
        RawComparator<BytesWritable> comp = new GroupComparator<BytesWritable>(new BytesWritable.Comparator(),new MerReduce.GroupMersWC());
        int[] mappings = new int[] { 4, 3, 0, 2, 1, 5 };
        RangePartitioner<BytesWritable,NullWritable> partitioner = new RangePartitioner<BytesWritable,NullWritable>(buckets,comp,mappings,excludeMask);
        
        BytesWritable myKey = new BytesWritable(new byte[] { (byte)0xff, (byte)0xee, 0x40, 0x00, 0x01 });
        
        System.out.println(partitioner.getPartition(myKey, null, mappings.length));

        myKey = new BytesWritable(new byte[] { (byte)0x66, (byte)0x39, 0x40, 0x00, 0x01 });
        
        System.out.println(partitioner.getPartition(myKey, null, mappings.length));

        myKey = new BytesWritable(new byte[] { (byte)0xff, (byte)0xef, 0x40, 0x00, 0x01 });
        
        System.out.println(partitioner.getPartition(myKey, null, mappings.length));

//        for ( BytesWritable bucket : buckets )
//            System.out.println(comp.compare(myKey, bucket));
    }
    */
}
