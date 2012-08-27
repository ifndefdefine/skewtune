package skewtune.mapred;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.SkewTuneJobConfig;
import skewtune.mapreduce.lib.input.InputSplitCache;

public class DelegateInputFormat<INKEY, INVALUE> implements InputFormat<INKEY, INVALUE>, Configurable, SkewTuneJobConfig {

    private InputFormat<INKEY,INVALUE> delegate; 
    
    public DelegateInputFormat() {}
    
    @Override
    public RecordReader<INKEY, INVALUE> getRecordReader(InputSplit split, JobConf conf,
            Reporter reporter) throws IOException {
        return delegate.getRecordReader(split,conf,reporter);
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int n) throws IOException {
        List<InputSplit> splits = InputSplitCache.get(conf);
        return splits.toArray(new InputSplit[splits.size()]);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void setConf(Configuration conf) {
        Class<? extends org.apache.hadoop.mapred.InputFormat> cls = 
              conf.getClass(ORIGINAL_INPUT_FORMAT_CLASS_ATTR,
                      org.apache.hadoop.mapred.InputFormat.class,
                      org.apache.hadoop.mapred.InputFormat.class);

        delegate = (org.apache.hadoop.mapred.InputFormat<INKEY,INVALUE>)
          ReflectionUtils.newInstance(cls, conf);
    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
