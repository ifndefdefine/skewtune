package skewtune.mapreduce.lib.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.SkewTuneJobConfig;

public class GroupComparator<K> implements RawComparator<K>, SkewTuneJobConfig {
    final RawComparator<K> sortComp;
    final RawComparator<K> grpComp;
    
    public GroupComparator(RawComparator<K> s,RawComparator<K> g) {
        sortComp = s;
        grpComp = g;
    }
    
    @Override
    public int compare(K k1, K k2) {
        return grpComp.compare(k1, k2) == 0 ? 0 : sortComp.compare(k1, k2);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return grpComp.compare(b1, s1, l1, b2, s2, l2) == 0 ? 0 : sortComp.compare(b1, s1, l1, b2, s2, l2);
    }
    
    
    public static RawComparator getInstance(JobConf conf) {
        if ( conf.get(JobContext.GROUP_COMPARATOR_CLASS) == null ) {
            return conf.getOutputKeyComparator();
        }
        return new GroupComparator(conf.getOutputKeyComparator(),conf.getOutputValueGroupingComparator());
    }
    
    public static RawComparator getInstance(MapContext context) {
        if ( context.getConfiguration().get(JobContext.GROUP_COMPARATOR_CLASS) == null ) {
            return context.getSortComparator();
        }
        return new GroupComparator(context.getSortComparator(),context.getGroupingComparator());
    }
    
    public static Class<?> getOriginalMapOutputKeyClass(Configuration conf) {
        return conf.getClass(ORIGINAL_MAP_OUTPUT_KEY_CLASS, conf.getClass(JobContext.OUTPUT_KEY_CLASS,null,Object.class), Object.class);  
    }
    
    public static RawComparator getOriginalSortComparator(Configuration conf) {
        Class<?> theClass = conf.getClass(ORIGINAL_MAP_OUTPUT_KEY_COMPARATOR_CLASS,null,RawComparator.class);
        if ( theClass == null ) {
            // it must be a default comparator
            return WritableComparator.get(getOriginalMapOutputKeyClass(conf).asSubclass(WritableComparable.class));
        }
        return (RawComparator) ReflectionUtils.newInstance(theClass, conf);
    }
    
    public static RawComparator getOriginalGroupComparator(Configuration conf) {
        Class<?> theClass = conf.getClass(ORIGINAL_GROUP_COMPARATOR_CLASS,null,RawComparator.class);
        if ( theClass == null ) {
            // it must be a default comparator
            return getOriginalSortComparator(conf);
        }
        return (RawComparator) ReflectionUtils.newInstance(theClass, conf);
    }
    
    public static RawComparator getOriginalInstance(Configuration conf) {
        Class<?> theClass = conf.getClass(ORIGINAL_GROUP_COMPARATOR_CLASS,null,RawComparator.class);
        if ( theClass == null ) {
            return getOriginalSortComparator(conf);
        }
        return new GroupComparator(getOriginalSortComparator(conf),getOriginalGroupComparator(conf));
    }
        
    /*
    public static RawComparator getInstance(Configuration conf) {
        Class<? extends RawComparator> theClass = conf.getClass(ORIGINAL_GROUP_COMPARATOR_CLASS, conf.getClass(JobContext.GROUP_COMPARATOR_CLASS,null,RawComparator.class), RawComparator.class);
        if (theClass == null) {
          return getSortComparator(conf);
        }
        return new GroupComparator(getSortComparator(conf),ReflectionUtils.newInstance(theClass, conf));
    }
    
    public static Class<?> getOutputKeyClass(Configuration conf) {
        return conf.getClass(JobContext.OUTPUT_KEY_CLASS,null,Object.class);
    }

    public static RawComparator getOutputKeyComparator(Configuration conf) {
        Class<? extends RawComparator> theClass = conf.getClass(JobContext.KEY_COMPARATOR, null, RawComparator.class);
        if ( theClass != null )
                return ReflectionUtils.newInstance(theClass, conf);
        return WritableComparator.get(getMapOutputKeyClass(conf).asSubclass(WritableComparable.class));
    }


    public static RawComparator getSortComparator(Configuration conf) {
        // we will instantiate a raw comparator for the original key and use it for comparison
        Class<? extends RawComparator> theClass = conf.getClass(ORIGINAL_MAP_OUTPUT_KEY_COMPARATOR_CLASS, null, RawComparator.class);
        if ( theClass == null )
            return getMapOutputKeyComparator(conf);
        return ReflectionUtils.newInstance(theClass, conf);
    }
    */
}