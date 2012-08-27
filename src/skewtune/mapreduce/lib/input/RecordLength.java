package skewtune.mapreduce.lib.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class RecordLength implements Writable {
    int keyLen;
    int valLen;
    int recLen;
    boolean splitable;
    
    public RecordLength() {}
    
    public void set(int key,int val,int rec,boolean split) {
        this.keyLen = key;
        this.valLen = val;
        this.recLen = rec;
        this.splitable = split;
    }
    
    public int getKeyLength() { return keyLen; }
    public int getValueLength() { return valLen; }
    public int getRecordLength() { return recLen; }
    public boolean isSplitable() { return splitable; }

    @Override
    public void readFields(DataInput in) throws IOException {
        keyLen = WritableUtils.readVInt(in);
        valLen = WritableUtils.readVInt(in);
        recLen = WritableUtils.readVInt(in);
        splitable = in.readBoolean();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, keyLen);
        WritableUtils.writeVInt(out, valLen);
        WritableUtils.writeVInt(out, recLen);
        out.writeBoolean(splitable);
    }
    
    @Override
    public String toString() {
        return String.format("k=%d,v=%d,r=%d,s=%b",keyLen,valLen,recLen,splitable);
    }
}
