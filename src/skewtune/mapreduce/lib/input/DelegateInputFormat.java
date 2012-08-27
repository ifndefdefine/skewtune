package skewtune.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.SkewTuneJobConfig;

/**
 * InputFormat reading keys, values from SequenceFiles in binary (raw)
 * format. InputSplits are passed through InputSplitCache.
 */
public class DelegateInputFormat<INKEY,INVALUE>
    extends CachedInputFormat<INKEY,INVALUE> implements Configurable, SkewTuneJobConfig {

    private InputFormat<INKEY,INVALUE> delegate;

  public DelegateInputFormat() {}

  @Override
  public Configuration getConf() {
      return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setConf(Configuration conf) {
      Class<? extends org.apache.hadoop.mapreduce.InputFormat> cls = 
            conf.getClass(ORIGINAL_INPUT_FORMAT_CLASS_ATTR,
                    org.apache.hadoop.mapreduce.InputFormat.class,
                    org.apache.hadoop.mapreduce.InputFormat.class);

      delegate = (org.apache.hadoop.mapreduce.InputFormat<INKEY,INVALUE>)
        ReflectionUtils.newInstance(cls, conf);
  }

  @Override
  public RecordReader<INKEY,INVALUE> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException {
    try {
		return delegate.createRecordReader(split,context);
	} catch (InterruptedException e) {
		throw new IOException(e);
	}
  }


    @Override
    public org.apache.hadoop.mapred.RecordReader<INKEY, INVALUE> getRecordReader(
            org.apache.hadoop.mapred.InputSplit split, JobConf job,
            Reporter reporter) throws IOException {
        throw new UnsupportedOperationException();
    }
}
