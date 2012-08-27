package skewtune.mapreduce.protocol;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TaskTrackerStatus implements Writable {
    String trackerName;
    String host;
    int httpPort;
    List<JobOnTaskTracker> taskStatus;
    long lastSeen;

    public TaskTrackerStatus() {
        this.taskStatus = new ArrayList<JobOnTaskTracker>();
    }

    public TaskTrackerStatus(String tr,String host,int httpPort,List<JobOnTaskTracker> status) {
        this.trackerName = tr;
        this.host = host;
        this.httpPort = httpPort;
        this.taskStatus = status;
    }

    public String getTrackerName() { return trackerName; }
    public String getHostName() { return host; }
    public int getHttpPort() { return httpPort; }
    public List<JobOnTaskTracker> getJobReports() { return taskStatus; }
    public long getLastSeen() { return lastSeen; }
    public void setLastSeen(long now) { lastSeen = now; }

    public void addStatus(JobOnTaskTracker status) {
        taskStatus.add(status);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        trackerName = Text.readString(in);
        host = Text.readString(in);
        httpPort = in.readUnsignedShort();
        taskStatus.clear();
        int numTasks = in.readInt();
        for ( int i = 0; i < numTasks; ++i ) {
            JobOnTaskTracker status = new JobOnTaskTracker();
            status.readFields(in);
            taskStatus.add(status);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, trackerName);
        Text.writeString(out, host);
        out.writeShort(httpPort);
        out.writeInt(taskStatus.size());
        for ( JobOnTaskTracker status : taskStatus ) {
            status.write(out);
        }
    }
    
    public static void main(String[] args) throws Exception {
        DataInputStream input = new DataInputStream(new FileInputStream(args[0]));
        TaskTrackerStatus status = new TaskTrackerStatus();
        status.readFields(input);
        input.close();
    }
}
