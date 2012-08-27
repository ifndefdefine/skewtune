package skewtune.mapreduce.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskAttemptID;

public class HeartbeatResponse implements Writable {
    private short responseId;
    private int heartbeatInterval;
    private ReactiveMapOutput[] newMapOutput;
//    private TaskAttemptID[] canceledTasks;
    private TaskAction[] taskActions;
    private JobID[] unknowns;
    private TaskStatusEvent[] takeover;

    public HeartbeatResponse() {}

//    public HeartbeatResponse(short respid,int interval,ReactiveMapOutput[] newOut,TaskAttemptID[] cancel,JobID[] unknowns) {
    public HeartbeatResponse(short respid,int interval,ReactiveMapOutput[] newOut,TaskAction[] actions,JobID[] unknowns,TaskStatusEvent[] takeover) {
        this.responseId = respid;
        this.heartbeatInterval = interval;
        this.newMapOutput = newOut;
//        this.canceledTasks = cancel;
        this.taskActions = actions;
        this.unknowns = unknowns;
        this.takeover = takeover;
    }

    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public short getResponseId() {
        return responseId;
    }

    public ReactiveMapOutput[] getNewMapOutputs() {
        return newMapOutput;
    }

//    public TaskAttemptID[] getCancelledTasks() {
//        return canceledTasks;
//    }
    public TaskAction[] getTaskActions() {
        return taskActions;
    }

    public JobID[] getUnknownJobs() {
        return unknowns;
    }
    
    public TaskStatusEvent[] getTakeovers() {
        return takeover;
    }

    public void readFields(DataInput in) throws IOException {
        responseId = in.readShort();
        heartbeatInterval = in.readInt();
        newMapOutput = new ReactiveMapOutput[in.readInt()];
        for ( int i = 0; i < newMapOutput.length; ++i ) {
            ReactiveMapOutput output = new ReactiveMapOutput();
            output.readFields(in);
            newMapOutput[i] = output;
        }
//        canceledTasks = new TaskAttemptID[in.readInt()];
//        for ( int i = 0; i < canceledTasks.length; ++i ) {
//            TaskAttemptID id = new TaskAttemptID();
//            id.readFields(in);
//            canceledTasks[i] = id;
//        }
        taskActions = new TaskAction[in.readInt()];
        for ( int i = 0; i < taskActions.length; ++i ) {
            TaskAction action = new TaskAction();
            action.readFields(in);
            taskActions[i] = action;
        }
        unknowns = new JobID[in.readInt()];
        for ( int i = 0; i < unknowns.length; ++i ) {
            JobID id = new JobID();
            id.readFields(in);
            unknowns[i] = id;
        }
        takeover = new TaskStatusEvent[in.readInt()];
        for ( int i = 0; i < takeover.length; ++i ) {
            TaskStatusEvent id = new TaskStatusEvent();
            id.readFields(in);
            takeover[i] = id;
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeShort(responseId);
        out.writeInt(heartbeatInterval);
        out.writeInt(newMapOutput.length);
        for ( ReactiveMapOutput output : newMapOutput ) {
            output.write(out);
        }
//        out.writeInt(canceledTasks.length);
//        for ( TaskAttemptID id : canceledTasks ) {
//            id.write(out);
//        }
        out.writeInt(taskActions.length);
        for ( TaskAction action : taskActions ) {
            action.write(out);
        }
        out.writeInt(unknowns.length);
        for ( JobID id : unknowns ) {
            id.write(out);
        }
        out.writeInt(takeover.length);
        for ( TaskStatusEvent id : takeover ) {
            id.write(out);
        }
    }

    public String toString() {
        return "";
    }
}
