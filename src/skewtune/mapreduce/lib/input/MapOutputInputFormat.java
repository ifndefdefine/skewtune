package skewtune.mapreduce.lib.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * retrieve MapOutput from task tracker.
 * 
 * @author yongchul
 *
 * @param <K>
 * @param <V>
 */
public class MapOutputInputFormat<K,V> extends CachedInputFormat<K,V> 
implements Configurable {
    
    public MapOutputInputFormat() {}
    
    @Override
    public void setConf(Configuration conf) {
    }

    @Override
    public Configuration getConf() {
        return null;
    }
    
    @Override
    public RecordReader<K,V> createRecordReader(InputSplit split,
            TaskAttemptContext context)
    throws IOException, InterruptedException {
        if ( !(split instanceof MapOutputSplit) )
            throw new IllegalArgumentException("Unsupported split type: "+split.getClass());
        
        MapOutputSplit msplit = (MapOutputSplit)split;
        
        // The shuffle will continuously read the data from socket and deflate. RecordReader 
        return msplit.isReactiveOutputSplit() ? new ReactiveMapOutputRecordReader<K,V>() : new TTMapOutputRecordReader<K,V>();
    }

    @Override
    public org.apache.hadoop.mapred.RecordReader<K, V> getRecordReader(
            org.apache.hadoop.mapred.InputSplit split, JobConf job,
            Reporter reporter) throws IOException {
        if ( !(split instanceof MapOutputSplit) )
            throw new IllegalArgumentException("Unsupported split type: "+split.getClass());
        
        MapOutputSplit msplit = (MapOutputSplit)split;
        try {
            if ( msplit.isReactiveOutputSplit() ) {
                ReactiveMapOutputRecordReader<K,V> reader = new ReactiveMapOutputRecordReader<K,V>();    
                reader.initialize(split, job,reporter);
                return reader;
            } else {
                TTMapOutputRecordReader<K,V> reader = new TTMapOutputRecordReader<K,V>();
                reader.initialize(split, job, reporter);
                return reader;
            }
        } catch (InterruptedException e ) {
            throw new IOException(e);
        }
    }
}