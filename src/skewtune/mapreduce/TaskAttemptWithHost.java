package skewtune.mapreduce;

import org.apache.hadoop.mapred.TaskAttemptID;

public class TaskAttemptWithHost {
    final TaskAttemptID attemptid;
    final String host;
    
    TaskAttemptWithHost(TaskAttemptID id,String host) {
        this.attemptid = id;
        this.host = host;
    }
    
    public TaskAttemptID getTaskAttemptID() { return attemptid; }
    public String getHost() { return host; }
}