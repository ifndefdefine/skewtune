package skewtune.mapreduce.lib.partition;

import java.io.DataInputStream;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The partitioner presents an illusion to delegating partitioner that there are X partitions rather than Y.
 * 
 * @author yongchul
 *
 * @param <K> Map output key type
 * @param <V> Map output value type
 */
public class DoubleHashDelegatePartitioner<K,V>
extends SkewTunePartitioner<K,V> {
    private static final Log LOG = LogFactory.getLog(DoubleHashDelegatePartitioner.class);
    int numVirtualReducers;
    int numReduces;
    Partitioner<K,V> delegate;

    @SuppressWarnings("unchecked")
    @Override
    public void setConf(Configuration conf) {
        Class<? extends Partitioner<K,V>> cls = (Class<? extends Partitioner<K, V>>) conf.getClass(ORIGINAL_PARTITIONER_CLASS_ATTR, Partitioner.class);
        delegate = ReflectionUtils.newInstance(cls, conf);
        numReduces = conf.getInt(JobContext.NUM_REDUCES, 1); // number of reduces

        // decode hash buckets
        DataInputStream input = null;
        try {
            input = getBucketInfo(conf);
            numVirtualReducers = WritableUtils.readVInt(input);
        } catch ( IOException e ) {
            throw new IllegalArgumentException(e);
        } finally {
            if ( input != null ) try { input.close(); } catch ( IOException ignore ) {}
        }
    }
    
    @Override
    public int getPartition(K key, V value, int n) {
        int part = delegate.getPartition(key, value, numVirtualReducers);
        return part % numReduces;
    }
}
