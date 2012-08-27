package skewtune.mapreduce.lib.partition;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.SkewTuneJobConfig;

/**
 * The partitioner presents an illusion to delegating partitioner that there are X partitions rather than Y.
 * 
 * @author yongchul
 *
 * @param <K> Map output key type
 * @param <V> Map output value type
 */
public class DelegatePartitioner<K,V>
extends SkewTunePartitioner<K,V>
implements Configurable, MRJobConfig, SkewTuneJobConfig {
    private static final Log LOG = LogFactory.getLog(DelegatePartitioner.class);
    int numVirtualReducers;
    Partitioner<K,V> delegate;
    
    @SuppressWarnings("unchecked")
    @Override
    public void setConf(Configuration conf) {
        DataInputStream input = null;
        try {
            input = this.getBucketInfo(conf);
            numVirtualReducers = input.readInt();
        } catch ( IOException ex ) {
            throw new IllegalArgumentException(ex);
        } finally {
            if ( input != null ) try { input.close(); } catch ( IOException ignore ) {}
        }
        
//        numOriginalReducers = conf.getInt(ORIGINAL_NUM_REDUCES, -1);
//        numVirtualReducers = conf.getInt(DELEGATE_PARTITIONER_NUM_VIRTUAL_REDUCES, -1);
//        LOG.info(String.format("# of original reducers = %d; # of virtual reducers = %d",numOriginalReducers,numVirtualReducers));
        LOG.info(String.format("# of virtual reducers = %d",numVirtualReducers));
        Class<? extends Partitioner<K,V>> cls = (Class<? extends Partitioner<K, V>>) conf.getClass(ORIGINAL_PARTITIONER_CLASS_ATTR, Partitioner.class);
        delegate = ReflectionUtils.newInstance(cls, conf);
    }
    
    @Override
    public int getPartition(K key, V value, int n) {
        return delegate.getPartition(key, value, numVirtualReducers) % n;
    }
}
