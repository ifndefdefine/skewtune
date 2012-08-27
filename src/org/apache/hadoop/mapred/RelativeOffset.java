package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class RelativeOffset implements Writable {
    public static final String INPUT_RELATIVE_OFFSET_ATTR = "map.input.relative.offset";
    public static final String INPUT_ORIGINAL_OFFSET_ATTR = "map.input.original.offset";
    
    long offset;
    int  numRecs;
    
    public RelativeOffset() {}
    
    public RelativeOffset(long o,int r) {
        this.offset = o;
        this.numRecs = r;
    }
    
    public RelativeOffset(Configuration conf) {
        String s = conf.get(INPUT_RELATIVE_OFFSET_ATTR);
        if ( s != null && s.length() > 0 ) {
            int idx = s.indexOf('@');
            offset = Long.parseLong(s.substring(0,idx));
            numRecs = Integer.parseInt(s.substring(idx+1));
        }
    }
    
    public String getConfString() {
        return offset + "@" + numRecs;
    }
    
    public void set(Configuration conf,long off) {
        if ( ! isEmpty() ) {
            conf.setLong(INPUT_ORIGINAL_OFFSET_ATTR, off);
            conf.set(INPUT_RELATIVE_OFFSET_ATTR, getConfString());
        }
    }

    public boolean shouldOverride(Configuration conf,long myoff) {
        if ( isEmpty() ) return false;
        long orgOff = conf.getLong(INPUT_ORIGINAL_OFFSET_ATTR,-1);
        return orgOff == myoff;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out,offset);
        WritableUtils.writeVInt(out, numRecs);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        offset = WritableUtils.readVLong(in);
        numRecs = WritableUtils.readVInt(in);
    }
    
    public long getOffset() { return offset; }
    public int getRecOffset() { return numRecs; }
    public boolean isEmpty() { return offset == 0 && numRecs == 0; }
    
    
    @Override
    public String toString() {
        return String.format("%d records from offset %d",numRecs, offset);
    }
}
