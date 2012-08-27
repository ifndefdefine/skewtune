package skewtune.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class MapOutputStat implements Writable {
    int numReduces;
    int[]  records;
    long[] bytes;

    // others?
    
    public MapOutputStat() {}
    
    public MapOutputStat(int n) {
        init(n);
    }
    
    private void init(int n) {
        numReduces = n;
        bytes = new long[numReduces];
        records = new int[numReduces];
    }
    
    public int getNumReduces() { return numReduces; }
    public int getNumRecords(int i) { return records[i]; }
    public long getNumBytes(int i) { return bytes[i]; }
    
    public void setNumRecords(int i,int v) { records[i] = v; }
    public void setNumBytes(int i,long v) { bytes[i] = v; }
    public void addRecords(int i,int v) { records[i] += v; }
    public void addBytes(int i,long v) { bytes[i] += v; }
    public void incRecord(int i) { ++records[i]; }

    @Override
    public void readFields(DataInput in) throws IOException {
        numReduces = in.readInt();
        bytes = new long[numReduces];
        records = new int[numReduces];
        for ( int i = 0; i < numReduces; ++i ) {
            records[i] = WritableUtils.readVInt(in);
        }
        for ( int i = 0; i < numReduces; ++i ) {
            bytes[i] = WritableUtils.readVLong(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(numReduces);
        for ( int r : records ) {
            WritableUtils.writeVInt(out, r);
        }
        for ( long b : bytes ) {
            WritableUtils.writeVLong(out, b);
        }
    }
}
