package skewtune.mapreduce.lib.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
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
public class MapOutputKeyInputFormat extends CachedInputFormat<BytesWritable,IntWritable> 
implements Configurable {
    
    public MapOutputKeyInputFormat() {}
    
    @Override
    public void setConf(Configuration conf) {
    }

    @Override
    public Configuration getConf() {
        return null;
    }
    
    @Override
    public RecordReader<BytesWritable,IntWritable> createRecordReader(InputSplit split,
            TaskAttemptContext context)
    throws IOException, InterruptedException {
        if ( !(split instanceof MapOutputSplit) )
            throw new IllegalArgumentException("Unsupported split type: "+split.getClass());
        
        MapOutputSplit msplit = (MapOutputSplit)split;
        
        // The shuffle will continuously read the data from socket and deflate. RecordReader 
        return msplit.isReactiveOutputSplit() ? new ReactiveMapOutputKeyRecordReader() : new TTMapOutputKeyRecordReader();
    }

    @Override
    public org.apache.hadoop.mapred.RecordReader<BytesWritable, IntWritable> getRecordReader(
            org.apache.hadoop.mapred.InputSplit split, JobConf job,
            Reporter reporter) throws IOException {
        if ( !(split instanceof MapOutputSplit) )
            throw new IllegalArgumentException("Unsupported split type: "+split.getClass());
        
        MapOutputSplit msplit = (MapOutputSplit)split;
        try {
            if ( msplit.isReactiveOutputSplit() ) {
                ReactiveMapOutputKeyRecordReader reader = new ReactiveMapOutputKeyRecordReader();    
                reader.initialize(split, job,reporter);
                return reader;
            } else {
                TTMapOutputKeyRecordReader reader = new TTMapOutputKeyRecordReader();
                reader.initialize(split, job, reporter);
                return reader;
            }
        } catch (InterruptedException e ) {
            throw new IOException(e);
        }
    }
}
