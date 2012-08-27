package skewtune.mapreduce.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TaskCostReport implements Writable, Cloneable {
    long dTime;
    long dRecords;
    long dBytes;
    long dKeyGroups;
    
    public TaskCostReport() {}
    
    public TaskCostReport(long t) {
        this.dTime = t;
    }

    public TaskCostReport(long t,long r,long b) {
        this.dTime = t;
        this.dRecords = r;
        this.dBytes = b;
    }

    public TaskCostReport(long t,long r,long b,long kg) {
        this.dTime = t;
        this.dRecords = r;
        this.dBytes = b;
        this.dKeyGroups = kg;
    }

    public synchronized void set(long t,long r,long b) {
        this.dTime = t;
        this.dRecords = r;
        this.dBytes = b;
    }
    
    public synchronized void set(long t,long r,long b,long kg) {
        this.dTime = t;
        this.dRecords = r;
        this.dBytes = b;
        this.dKeyGroups = kg;
    }
    
    public synchronized TaskCostReport getLastReport(long begin) {
        return new TaskCostReport(dTime - begin,dRecords,dBytes,dKeyGroups);
    }

    public TaskCostReport getDelta(long t,long r,long b) {
        TaskCostReport delta = new TaskCostReport(t - dTime,r - dRecords,b - dBytes);
        this.set(t, r, b);
        return delta;
    }

    public TaskCostReport getDelta(long t,long r,long b,long kg) {
        if ( kg == dKeyGroups )
            return null;
        
        TaskCostReport delta = new TaskCostReport(t - dTime,r - dRecords,b - dBytes,kg - dKeyGroups);
        this.set(t, r, b, kg);
        return delta;
    }
    

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out,dTime);
        WritableUtils.writeVLong(out,dRecords);
        WritableUtils.writeVLong(out,dBytes);
        WritableUtils.writeVLong(out,dKeyGroups);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dTime = WritableUtils.readVLong(in);
        dRecords = WritableUtils.readVLong(in);
        dBytes = WritableUtils.readVLong(in);
        dKeyGroups = WritableUtils.readVLong(in);
    }
    
    public double getTime() { return dTime * 0.001; }
    public long getNumRecords() { return dRecords; }
    public long getNumBytes() { return dBytes; }
    public long getNumKeyGroups() { return dKeyGroups; }
    
    @Override
    public synchronized Object clone() {
        try {
            return super.clone();
        } catch ( CloneNotSupportedException ex ) {
            throw new InternalError(ex.toString());
        }
    }

    @Override
    public String toString() {
        return "CostReport:"+dRecords+" records "+dBytes+" bytes "+dKeyGroups+" keys in "+getTime()+" secs"; 
    }
}
