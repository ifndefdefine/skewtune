package skewtune.mapreduce.protocol;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.MapOutputIndex;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapreduce.TaskType;

import skewtune.mapreduce.MapOutputStat;
import skewtune.mapreduce.TaskProgress;

public class STTaskStatus implements Writable, Cloneable {
    private final TaskAttemptID taskid;
    private volatile TaskStatus.Phase phase = TaskStatus.Phase.STARTING;
    private volatile TaskStatus.State runState = TaskStatus.State.RUNNING;
    
    private long remainTimeAt; // time when remaining time captured
    private long remainTime; // remaining time from remainTimeAt
    private long timePassed; // how long does it passed at remainTimeAt? in milliseconds
    private float progress; // progress of given phase
    private float timePerByte; // time per bytes
    
//    private long startTime; // time since it actually started to running
//    private float progress;

//    private TaskProgress progress;

    /**
     * optional map output statistics
     */
    private MapOutputStat stat;
    private volatile MapOutputIndex index; // map output index. only assigned when it is reactive map task.
    private volatile TaskCostReport costReport; // cost report. sent every minute and when task completes

    public STTaskStatus() {
        this.taskid = new TaskAttemptID();
    }

    public STTaskStatus(TaskAttemptID id) {
        this.taskid = id;
    }

//    public STTaskStatus(TaskAttemptID id,float progress,MapOutputStat stat) {
//        this.taskid = id;
//        this.progress = progress;
//        this.stat = stat;
//    }

    public TaskAttemptID getTaskID() { return taskid; }
//    public float getProgress() { return progress; }
    public MapOutputStat getMapOutputStat() { return stat; }
    public MapOutputIndex getMapOutputIndex() { return index; }
    public TaskType getTaskType() { return taskid.getTaskType(); }
    public TaskCostReport getCostReport() { return costReport; }

//    public void setProgress(float p) { progress = p; }
    public void setRemainTime(long at,long t) {
        setRemainTime(at,t,0);
    }
    public void setRemainTime(long at,long t,long p) {
        remainTimeAt = at;
        remainTime = t;
        timePassed = p;
    }
    
    public long getRemainTimeAt() {
        return remainTimeAt;
    }
    public long getRemainTime() {
        return remainTime;
    }
    public long getTimePassed() {
        return timePassed;
    }
    public double getRemainTime(long now) {
        if ( remainTime == 0 ) return 0;
        return (remainTime - (now - remainTimeAt))*0.001;
    }
    public float getTimePerByte() {
        return timePerByte;
    }
    public double getTimePassed(long now) {
        return (now - (remainTimeAt - timePassed))*0.001;
    }
    
    public void setMapOutputStat(MapOutputStat s) {
        if ( stat == null ) {
            stat = s;
        }
    }
    public void setMapOutputIndex(MapOutputIndex index) {
        if ( this.index == null ) {
            this.index= index;
        }
    }

    public TaskStatus.Phase getPhase() { return phase; }
    public float getProgress() { return progress; }
    public TaskStatus.State getRunState() { return runState; }
    public void setPhase(TaskStatus.Phase phase) {
        this.phase = phase;
    }
    public void setProgress(float v) {
        this.progress = v;
    }
    public void setTimePerByte(float v) {
        this.timePerByte = v;
    }
    public void setRunState(TaskStatus.State state) { this.runState = state; }
    public void setCostReport(TaskCostReport report) { this.costReport = report; }
    
    // in case of reduce, adjust the progress and normalize
    /*
    public float adjustedProgress() {
        if ( taskid.getTaskType() == TaskType.REDUCE ) {
            // each phase in reduce is equally weighted.
            if ( phase == Phase.SORT ) {
                return (progress - 0.33f) * 3.0f;
            } else if ( phase == Phase.REDUCE ) {
                return (progress - 0.66f) * 3.0f;
            }
        }
        return progress;
    }
    */
    
    /*
    public double getCurrentProgressRate(long now) {
        return  adjustedProgress() / ((now - startTime)/1000.);
//        return progress.getCurrentProgressRate(now);
    }
    
    public double getEstimateTime(long now) {
        double estTime = Math.max(0.0001, 1.0 - adjustedProgress()) / getCurrentProgressRate(now);
        return adjustEstimateTime(now,estTime);
//        return Math.max(0.0001, 1.0 - progress) / getCurrentProgressRate(now);
//        return progress.getEstimateTime(now);
    }
    
    private double adjustEstimateTime(long now,double t) {
        int remainingPhase = 0;
        if ( taskid.getTaskType() == TaskType.MAP ) {
            switch ( phase ) {
                case STARTING: remainingPhase = 2; break;
                case MAP: remainingPhase = 1; break;
            }
        } else {
            switch ( phase ) {
                case STARTING: remainingPhase = 3; break;
                case SHUFFLE: remainingPhase = 2; break;
                case SORT: remainingPhase = 1; break;
            }
        }
        
        if ( remainingPhase > 0 ) {
            double est = ((now - startTime)/1000.) + t;
            t += est * remainingPhase;
        }
        return t;
    }
    */
    
    
    public synchronized void statusUpdate(STTaskStatus status) {
        if ( ! this.taskid.equals(status.taskid) ) {
            throw new IllegalArgumentException();
        }
        setPhase( status.getPhase() );
        setRunState( status.getRunState() );
        setProgress( status.getProgress() );
        setTimePerByte( status.getTimePerByte() );
        setRemainTime( status.getRemainTimeAt(), status.getRemainTime(), status.getTimePassed() );
//        setProgress( status.getProgress() );
        setMapOutputStat( status.getMapOutputStat() );
        setMapOutputIndex( status.getMapOutputIndex() );
        setCostReport( status.getCostReport() );
//        setStartTime( status.getStartTime() );
    }

    public synchronized void statusUpdate(TaskStatus.Phase phase,float progress,TaskStatus.State state,long at,long t) {
        setPhase( phase );
        setRunState( state);
        setRemainTime( at, t );
        setProgress( progress );
    }

    public synchronized void statusUpdate(TaskStatus.Phase phase,float progress,TaskStatus.State state,long at,long t,long p,MapOutputStat stat) {
        setPhase( phase );
        setRunState( state);
        setRemainTime(at, t, p);
        setProgress( progress );
        setMapOutputStat(stat);
    }
    
    public synchronized void statusUpdate(TaskStatus.Phase phase,float progress,TaskStatus.State state,long at,long t,MapOutputStat stat,MapOutputIndex index) {
        setPhase( phase );
        setRunState( state);
        setRemainTime( at, t);
        setProgress( progress );
        setMapOutputStat(stat);
        setMapOutputIndex(index);
    }

    public synchronized void statusUpdate(TaskStatus taskStatus,float progress, long at, long t, long p, float r,TaskCostReport report) {
        if ( ! this.taskid.equals(taskStatus.getTaskID() ) ) {
            throw new IllegalArgumentException();
        }
        setPhase( taskStatus.getPhase() );
        setProgress( progress );
        setTimePerByte( r );
        setRunState( taskStatus.getRunState() );
        setRemainTime( at, t, p );
        setProgress( progress );
        setCostReport( report );
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.taskid.readFields(in);
        this.runState = WritableUtils.readEnum(in, TaskStatus.State.class);
        this.phase = WritableUtils.readEnum(in, TaskStatus.Phase.class);
        this.progress = in.readFloat();
        this.timePerByte = in.readFloat();
        
//        this.startTime = in.readLong();
//        this.progress = in.readFloat();
        this.remainTimeAt = in.readLong();
        this.remainTime = WritableUtils.readVLong(in);
        this.timePassed = WritableUtils.readVLong(in);
        
        byte flag = in.readByte();
        if ( (flag & 0x01) != 0 ) { 
            this.stat = new MapOutputStat();
            this.stat.readFields(in);
        } else {
            this.stat = null;
        }
        if ( (flag & 0x02) != 0 ) {
            this.index = new MapOutputIndex();
            this.index.readFields(in);
        } else {
            this.index = null;
        }
        if ( (flag & 0x04) != 0 ) {
            this.costReport = new TaskCostReport();
            costReport.readFields(in);
        } else {
            costReport = null;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.taskid.write(out);
        WritableUtils.writeEnum(out,this.runState);
        WritableUtils.writeEnum(out,this.phase);
        out.writeFloat(progress);
        out.writeFloat(timePerByte);
        
//        out.writeLong(startTime);
//        out.writeFloat(this.progress);
        out.writeLong(remainTimeAt);
        WritableUtils.writeVLong(out,remainTime);
        WritableUtils.writeVLong(out, timePassed);
        
//        progress.write(out);
        
        byte flag = (byte) (( this.stat == null ) ? 0x00 : 0x01);
        flag |= (byte) (( this.index == null ) ? 0x00 : 0x02);
        flag |= (byte) (( this.costReport == null ) ? 0x00 : 0x04);
        
        out.writeByte(flag);
        
        if ( (flag & 0x01) != 0 ) {
            this.stat.write(out);
        }
        if ( (flag & 0x02) != 0 ) {
            this.index.write(out);
        }
        if ( (flag & 0x04) != 0 ) {
            this.costReport.write(out);
        }
    }

    @Override
    public synchronized Object clone() {
        try {
            return super.clone();
        } catch ( CloneNotSupportedException ex ) {
            throw new InternalError(ex.toString());
        }
    }
    
    @Override
    public int hashCode() { return taskid.hashCode(); }
    
    @Override
    public boolean equals(Object o) {
        if ( this == o ) return true;
        if ( o instanceof STTaskStatus ) {
            return taskid.equals(((STTaskStatus)o).taskid);
        }
        return false;
    }
    
    public static void main(String[] args) throws Exception {
        DataInputStream input = new DataInputStream(new FileInputStream(args[0]));
        STTaskStatus status = new STTaskStatus();
        status.readFields(input);
        input.close();
    }

//    public synchronized void setStartTime(long t) {
//        startTime = t;
//    }
//    public long getStartTime() { return startTime; }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        
        buf.append(taskid).append(' ');
        buf.append(phase).append(' ');
        buf.append(progress).append(' ');
        buf.append(timePerByte).append(' ');
        buf.append(runState).append(' ');
//        buf.append(progress);
//        buf.append(startTime).append(' ');
//        buf.append(progress);
        buf.append(remainTimeAt).append(' ');
        buf.append(remainTime);
        
        return buf.toString();
    }
}
