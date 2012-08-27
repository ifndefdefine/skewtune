package skewtune.mapreduce.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;

import skewtune.mapreduce.TaskState;

public class TaskStatusEvent implements Writable, Comparable<TaskStatusEvent> {
    TaskID taskid;
    TaskState status;
    
    public TaskStatusEvent() {
        taskid = new TaskID();
    }
    
    public TaskStatusEvent(TaskAttemptID id,TaskState t) {
        taskid = id.getTaskID();
        status = t;
    }
    public TaskStatusEvent(TaskID id,TaskState t) {
        taskid = id;
        status = t;
    }
    
    public TaskStatusEvent(TaskStatusEvent e) {
        taskid = e.taskid;
        status = e.status;
    }

    public JobID  getJobID() { return taskid.getJobID(); }
    public TaskID getTaskID() { return taskid; }
    public TaskState getState() { return status; }

    @Override
    public void write(DataOutput out) throws IOException {
        taskid.write(out);
        out.writeByte(status.ordinal() & 0x0f);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        taskid.readFields(in);
        int n = in.readByte();
        status = TaskState.values()[n];
    }
    
    @Override
    public String toString() {
        return String.format("%s status=%s",taskid,status.toString());
    }
    
    @Override
    public int hashCode() {
        return taskid.hashCode();
    }

    @Override
    public int compareTo(TaskStatusEvent o) {
        return taskid.compareTo(o.taskid);
    }
    
    public Internal toInternal() {
        return new Internal(this);
    }
    
    public static class Internal implements Writable {
        int partition;
        TaskState state;

        public Internal() {
        }

        public Internal(TaskStatusEvent e) {
            partition = e.getTaskID().getId();
            state = e.getState();
        }

        public int getPartition() { return partition; }
        public TaskState getState() { return state; }
        
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(partition);
            out.writeByte(state.ordinal() & 0x0f);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            partition = in.readInt();
            int n = in.readByte();
            state = TaskState.values()[n];
        }
    }
}
