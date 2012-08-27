package skewtune.mapred;

import java.io.DataInputStream;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.lib.partition.SkewTunePartitioner;

/**
 * The partitioner presents an illusion to delegating partitioner that there are X partitions rather than Y.
 * 
 * @author yongchul
 *
 * @param <K> Map output key type
 * @param <V> Map output value type
 */
public class RehashDelegatePartitioner<K,V>
extends SkewTunePartitioner<K,V> {
    private static final Log LOG = LogFactory.getLog(RehashDelegatePartitioner.class);
    int numOriginalReducers;
    int numVirtualReducers;
    Partitioner<K,V> delegate;
    int[] buckets;
    int[] mapping;

    @SuppressWarnings("unchecked")
    @Override
    public void setConf(Configuration conf) {
        Class<? extends Partitioner<K,V>> cls = (Class<? extends Partitioner<K, V>>) conf.getClass(ORIGINAL_PARTITIONER_CLASS_ATTR, Partitioner.class);
        delegate = ReflectionUtils.newInstance(cls, conf);
        int numReduces = conf.getInt(JobContext.NUM_REDUCES, 1); // number of reduces

        // decode hash buckets
        DataInputStream input = null;
        try {
            input = getBucketInfo(conf);
            buckets = new int[numReduces-1];
            for ( int i = 0; i < buckets.length; ++i ) {
                buckets[i] = WritableUtils.readVInt(input);
            }
            numVirtualReducers = WritableUtils.readVInt(input);
            
            mapping = new int[numReduces];
            for ( int i = 0; i < mapping.length; ++i ) {
                mapping[i] = WritableUtils.readVInt(input);
            }
        } catch ( IOException e ) {
            throw new IllegalArgumentException(e);
        } finally {
            if ( input != null ) try { input.close(); } catch ( IOException ignore ) {}
        }
    }
    
    @Override
    public int getPartition(K key, V value, int n) {
        int part = delegate.getPartition(key, value, numVirtualReducers);
        for ( int i = 0; i < buckets.length; ++i ) {
            if ( part < buckets[i] ) return mapping[i];
        }
        return mapping[n-1];
    }
}
