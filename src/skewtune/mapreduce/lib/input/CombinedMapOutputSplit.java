package skewtune.mapreduce.lib.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;

public class CombinedMapOutputSplit extends InputSplit implements Writable, Iterable<MapOutputSplit>, org.apache.hadoop.mapred.InputSplit {
    private JobID jobid;
    private HashMap<String,MapOutputSplit> host2split = new HashMap<String,MapOutputSplit>();
    private ArrayList<MapOutputSplit> splits = new ArrayList<MapOutputSplit>();
    
    public CombinedMapOutputSplit() {}
    
    public void add(MapOutputSplit split) {
        if ( splits.isEmpty() ) {
            jobid = split.getJobId();
        } else if ( ! jobid.equals( split.getJobId() ) ) {
            throw new IllegalArgumentException("job id does not match. expected: "+jobid);
        } else if ( host2split.containsKey( split.getHost() ) ) {
            // FIXME generate warning
        }
        splits.add(split);
        host2split.put(split.getHost(), split);
    }

    public Set<String> getHosts() {
        return host2split.keySet();
    }
    
    public JobID getJobId() {
        return jobid;
    }
            
    @Override
    public long getLength() {
        long sz = 0;
        for ( MapOutputSplit s : splits )
            sz += s.getLength();
        return sz;
    }

    @Override
    public String[] getLocations() {
        return host2split.keySet().toArray(new String[splits.size()]);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int n = in.readUnsignedByte();
        splits.ensureCapacity(n);
        splits.clear();
        for ( int i = 0; i < n; ++i ) {
            MapOutputSplit s = new MapOutputSplit();
            s.readFields(in);
            splits.add(s);
        }
        jobid = splits.get(0).getJobId();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(splits.size()); // FIXME splits should not exceed 128
        for ( MapOutputSplit split : splits ) {
            split.write(out);
        }
    }
    
    @Override
    public Iterator<MapOutputSplit> iterator() {
        return splits.iterator();
    }
}
