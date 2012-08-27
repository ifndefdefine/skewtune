package skewtune.mapreduce.lib.input;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * InputFormat reading keys, values from SequenceFiles in binary (raw)
 * format. InputSplits are passed through InputSplitCache.
 */
public abstract class CachedInputFormat<INKEY,INVALUE>
    extends InputFormat<INKEY,INVALUE> implements org.apache.hadoop.mapred.InputFormat<INKEY, INVALUE>{

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,InterruptedException {
      int key = InputSplitCache.getCacheKey(context.getConfiguration());
      return InputSplitCache.remove(key);
  }

  public static int setSplits(JobContext context,InputSplit[] splits) throws IOException {
      return InputSplitCache.set(context.getConfiguration(),Arrays.asList(splits));
  }

  public static int setSplits(JobContext context,List<InputSplit> splits) throws IOException {
      return InputSplitCache.set(context.getConfiguration(),splits);
  }


      public static void removeCache(Configuration conf) throws IOException {
          int key = InputSplitCache.getCacheKey(conf);
          if ( key >= 0 ) {
              InputSplitCache.remove(key);
          }
      }

      public static int setSplits(JobConf conf,org.apache.hadoop.mapred.InputSplit[] splits) throws IOException {
          return InputSplitCache.set(conf, Arrays.asList(splits));
      }
      
      public static int setSplits(JobConf conf,List<org.apache.hadoop.mapred.InputSplit> splits) throws IOException {
          return InputSplitCache.set(conf, splits);
      }

    @Override
    public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf job,int numSplits) throws IOException {
        int key = InputSplitCache.getCacheKey(job);
        List<org.apache.hadoop.mapred.InputSplit> splits = InputSplitCache.remove(key);
        return splits.toArray(new org.apache.hadoop.mapred.InputSplit[splits.size()]);
    }
}
