package skewtune.mapreduce.protocol;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.mapred.TaskAttemptID;

import skewtune.mapreduce.MapOutputStat;

public interface SkewTuneTaskUmbilicalProtocol extends VersionedProtocol {
  public static final long versionID = 1L;

  /**
   * return code of init/done/ping/statusUpdate
   */
  public static final int RESP_OK = 0;
  public static final int RESP_CANCEL = 1;
  public static final int RESP_REPORT = 2;
  public static final int RESP_REPORT_AND_STOP = 3;
  
  /**
   * task has been initialized
   * 
   * @param taskId
   * @return
   * @throws IOException
   */
  int init(TaskAttemptID taskId,int nMap,int nReduce) throws IOException;

  /**
   * task has completed.
   * 
   * @param taskId
   * @return
   * @throws IOException
   */
  int done(TaskAttemptID taskId) throws IOException;
  
  /**
   * periodically called to check whether this task should be canceled.
   * 
   * @param taskId
   * @return false if the task should cancel
   * @throws IOException
   */
  int ping(TaskAttemptID taskId) throws IOException;

  /**
   * at the end of map task, send the statistics of Map output.
   * per reducer, the size of map output and the number of records are tracked.
   * 
   * @param taskid
   * @param stat
   * @return
   * @throws IOException
   */
  int statusUpdate(TaskAttemptID taskid,STTaskStatus status) throws IOException;
  
  void setSplitInfo(TaskAttemptID taskid,byte[] data) throws IOException;
  
  // TODO
  // an API to retrieve reactive MAP OUTPUT for reduce
  // Input: my task id
  // Output: available output
  //   HDFS DIR
  //     map id (int), offset (long), length (long)
  //     ...
  
  MapOutputUpdates getCompltedMapOutput(TaskAttemptID taskid,int from,int fromTakeover) throws IOException;
  
  
}
