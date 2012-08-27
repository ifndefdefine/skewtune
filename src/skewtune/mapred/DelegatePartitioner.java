package skewtune.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.SkewTuneJobConfig;

/**
 * delegate partitions on Map split
 * 
 * @author yongchul
 *
 * @param <K>
 * @param <V>
 */
public class DelegatePartitioner<K,V> implements Partitioner<K, V>, SkewTuneJobConfig {
    private static final Log LOG = LogFactory.getLog(DelegatePartitioner.class);

    private Partitioner<K,V> delegate;
    int numOriginalReducers;
    int numVirtualReducers;

    @Override
    @SuppressWarnings("unchecked")
    public void configure(JobConf conf) {
        numOriginalReducers = conf.getInt(ORIGINAL_NUM_REDUCES, -1);
        numVirtualReducers = conf.getInt(DELEGATE_PARTITIONER_NUM_VIRTUAL_REDUCES, -1);
        LOG.info(String.format("# of original reducers = %d; # of virtual reducers = %d",numOriginalReducers,numVirtualReducers));
        Class<? extends Partitioner<K,V>> cls = (Class<? extends Partitioner<K, V>>) conf.getClass(ORIGINAL_PARTITIONER_CLASS_ATTR, Partitioner.class);
        delegate = ReflectionUtils.newInstance(cls, conf);
    }

    @Override
    public int getPartition(K key, V value, int n) {
        return delegate.getPartition(key, value, numVirtualReducers) % n;
    }
}
