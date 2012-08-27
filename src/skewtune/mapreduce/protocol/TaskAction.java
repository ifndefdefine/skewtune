package skewtune.mapreduce.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.TaskAttemptID;

public class TaskAction implements Writable {
    // return code encoding
    // top most bit -- stop
    // top most bit-1 -- include this task
    // report -- conveys the number of new partition
    
    // okay, cancel, split -- 2bits (cancel/include) the rest is the number of partitions

    public static final int FLAG_CANCEL  = 0x80000000;
    public static final int FLAG_SPLIT   = 0x40000000;
    public static final int FLAG_INCLUDE = 0x20000000;
    public static final int MASK_STATUS  = 0xE0000000; // 0: okay, or cancel or split
    public static final int MASK_PARTITIONS = ~MASK_STATUS;
    
    private TaskAttemptID attemptId;
    private int action;
    
    public TaskAction() {
        attemptId = new TaskAttemptID();
    }
    
    public TaskAction(TaskAttemptID id,int action) {
        this.attemptId = id;
        this.action = action;
    }

    public TaskAction(TaskAttemptID id,int n,boolean incl) {
        this.attemptId = id;
        this.split(n,incl);
    }
    
    public TaskAttemptID getAttemptID() { return attemptId; }
    public int getAction() { return action; }
    public boolean isCanceled() { return (action & FLAG_CANCEL) != 0; }
    public void cancel() {
        action = FLAG_CANCEL;
    }
    public void split(int n) {
        action = FLAG_SPLIT | n;
    }
    public void split(int n,boolean incl) {
        action = FLAG_SPLIT|FLAG_INCLUDE|n;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        attemptId.write(out);
        out.writeInt(action);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        attemptId.readFields(in);
        action = in.readInt();
    }
    
    @Override
    public int hashCode() { return attemptId.hashCode(); }
    
    @Override
    public String toString() {
        return attemptId.toString() + " action = " + action;
    }
}
