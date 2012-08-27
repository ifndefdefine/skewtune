package skewtune.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskType;

public class TaskProgress implements Writable, Cloneable {
    long startTime;
    int currentPhase;
    int[] relTime; // time since start time
    float[] progress;
    
    protected TaskProgress(int n) {
        relTime = new int[n];
        progress = new float[n];
    }
    
    public TaskProgress() {}
    
    public synchronized void start() {
        startTime = System.currentTimeMillis();
    }
    
    public synchronized void set(TaskProgress o) {
        this.startTime = o.startTime;
        currentPhase = o.currentPhase;
        System.arraycopy(o.relTime,0,relTime,0,o.relTime.length);
        System.arraycopy(o.progress,0,progress,0,progress.length);
    }

    @Override
    public synchronized void write(DataOutput out) throws IOException {
        out.writeByte(relTime.length);
        out.writeLong(startTime);
        out.writeInt(currentPhase);
        for ( int i = 0; i < currentPhase; ++i ) {
            out.writeInt(relTime[i]);
            out.writeFloat(progress[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte sz = in.readByte();
        if ( relTime == null ) relTime = new int[sz];
        if ( progress == null ) progress = new float[sz];
        startTime = in.readLong();
        currentPhase = in.readInt();
        for ( int i = 0; i < currentPhase; ++i ) {
            relTime[i] = in.readInt();
            progress[i] = in.readFloat();
        }
    }
    
    public synchronized void startNextPhase() {
        ++currentPhase;
        relTime[currentPhase] = (int)(System.currentTimeMillis() - startTime);
    }
    
    public static TaskProgress getIntance(TaskType type) {
        return type == TaskType.MAP ? new MapTaskProgress() : new ReduceTaskProgress();
    }

    /**
     * progress rate of current phase
     * @param now
     * @return
     */
    public synchronized double getCurrentProgressRate(long now) {
        return progress[currentPhase] / Math.max(1,((now - startTime) - relTime[currentPhase]));
    }
    
    public synchronized double getEstimateTime(long now) {
        double rt =  Math.max(0.0001, 1.0 - progress[currentPhase]) / Math.min(0.001,getCurrentProgressRate(now));
        return adjustRemainingTime(now,rt);
    }
    
    /**
     * get adjust remaining time assuming 1) current progress information, 2)
     * given remaining time for current phase
     * 
     * @param now current time
     * @param rt
     * @return
     */
    private double adjustRemainingTime(long now,double rt) {
        // assume the rest of phase taking the same time as current phase
        if ( currentPhase == relTime.length-1 ) return rt;
        double estCurrentPhase = ((((now - startTime) - relTime[currentPhase])/1000.) + rt);
        return  rt + estCurrentPhase * (relTime.length - currentPhase - 1);
    }
    
    @Override
    public synchronized Object clone() {
        try {
            TaskProgress copy = (TaskProgress)super.clone();
            copy.relTime = relTime.clone();
            copy.progress = progress.clone();
            return copy;
        } catch (CloneNotSupportedException e) {
        }
        return null;
    }
    
    public static class MapTaskProgress extends TaskProgress {
        public MapTaskProgress() {
            // START, MAP, SORT
            super(3);
        }
    }

    public static class ReduceTaskProgress extends TaskProgress {
        public ReduceTaskProgress() {
            // START, SHUFFLE, SORT, REDUCE
            super(4);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("start=").append(startTime).append(' ');
        buf.append("current=").append(currentPhase).append(' ');
        buf.append("relTime=").append(Arrays.toString(relTime)).append(' ');
        buf.append("progress=").append(Arrays.toString(progress));
        return buf.toString();
    }
}
