package skewtune.mapreduce.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobID;

public class JobOnTaskTracker implements Writable {
    private JobID jobid;
    /**
     * retrieve map output (or completion event) from given index. -1 when this is not requested.
     */
    private int fromIndex = -1;
    private int fromIndexTakeOver = -1;
    private static final SRTaskStatus[] EMPTY_STATUS = new SRTaskStatus[0];
    
    private SRTaskStatus[] tasks = EMPTY_STATUS; // tasks belong to this job
    
    public JobOnTaskTracker() {
        this.jobid = new JobID();
    }

    public JobOnTaskTracker(JobID jobid,int from) {
        this.jobid = jobid;
        this.fromIndex = from;
    }
    
    public JobOnTaskTracker(JobID jobid,int from,Collection<SRTaskStatus> status) {
        this.jobid = jobid;
        this.fromIndex = from;
        this.tasks = status.toArray(new SRTaskStatus[status.size()]);
    }
    
    public JobOnTaskTracker(JobID jobid,int from,Collection<SRTaskStatus> status,int takeOverIndex) {
        this.jobid = jobid;
        this.fromIndex = from;
        this.tasks = status.toArray(new SRTaskStatus[status.size()]);
        this.fromIndexTakeOver = takeOverIndex;
    }

    public JobID getJobID() { return jobid; }
    public int getFromIndex() { return fromIndex; }
    public void setFromIndex(int i) { fromIndex = i; }
    public boolean pollMapOutput() { return fromIndex >= 0; }
    
    public int getFromIndexOfTakeOver() { return fromIndexTakeOver; }
    public void setFromIndexOfTakeOver(int i) { fromIndexTakeOver = i; }
    public boolean pollTakeOver() { return fromIndexTakeOver >= 0; }
    
    public SRTaskStatus[] getTaskReports() { return tasks; }
    public void setTaskReports(Collection<SRTaskStatus> x) {
        tasks = x.toArray(new SRTaskStatus[x.size()]);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        jobid.readFields(in);
        fromIndex = in.readInt();
        tasks = new SRTaskStatus[in.readInt()];
        for ( int i = 0; i < tasks.length; ++i ) {
            SRTaskStatus status = new SRTaskStatus();
            status.readFields(in);
            tasks[i] = status;
        }
        fromIndexTakeOver = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        jobid.write(out);
        out.writeInt(fromIndex);
        out.writeInt(tasks.length);
        for ( SRTaskStatus status : tasks ) {
            status.write(out);
        }
        out.writeInt(fromIndexTakeOver);
    }
}
