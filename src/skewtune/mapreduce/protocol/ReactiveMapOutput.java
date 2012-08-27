package skewtune.mapreduce.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapOutputIndex;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;

/**
 * new map output for reactive map. used between tracker.
 * @author yongchul
 *
 */
public class ReactiveMapOutput implements Writable {
    JobID jobid;
    int   partition; // we know we are working on Map!
    int   nIdx; // there will be this many indexes
    int   nOldReduces;
    MapOutputIndex   index;
    boolean speculative;
    
    public ReactiveMapOutput() {
        jobid = new JobID();
        index = new MapOutputIndex();
    }
    
    public ReactiveMapOutput(TaskID taskid,int nNewMap,int nOldReduces,boolean speculative) {
        jobid = taskid.getJobID();
        partition = taskid.getId();
        nIdx = nNewMap;
        index = new MapOutputIndex(nNewMap*nOldReduces);
        this.nOldReduces = nOldReduces;
        this.speculative = speculative;
    }

    public int getPartition() { return partition; }
    public JobID getJobID() { return jobid; }
    public int getNumIndexes() { return nIdx; }
    public int getNumReduces() { return nOldReduces; }
    public boolean isSpeculative() { return speculative; }
    
    public void setMapOutputIndex(int mapNum,MapOutputIndex idx) {
        if ( idx == null )
            throw new IllegalArgumentException("mapoutput index is null: partition "+mapNum);
        if ( idx.size() != nOldReduces ) {
            throw new IllegalArgumentException(String.format("map output index size does not match. expected = %d, given = %d",nOldReduces,idx.size()));
        }
        this.index.put(mapNum, idx);
    }
    
    public MapOutputIndex getOutputIndex(int reduce) {
        // slice MapOutputIndex for given reduce
        MapOutputIndex r = new MapOutputIndex(nIdx);
        for ( int i = 0; i < nIdx; ++i, reduce += nOldReduces ) {
            r.copyIndex(index, reduce, i);
        }
        return r;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        jobid.write(out);
        out.writeInt(partition);
        out.writeInt(nIdx);
        out.writeInt(nOldReduces);
        out.writeBoolean(speculative);
        index.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        jobid.readFields(in);
        partition = in.readInt();
        nIdx = in.readInt();
        nOldReduces = in.readInt();
        speculative = in.readBoolean();
        index.readFields(in);
    }
    
    @Override
    public String toString() {
//        return String.format("%s map %d %d splits for %d reduces\n%s", jobid, partition, nIdx, nOldReduces, index);
        return String.format("%s %s map %d %d splits for %d reduces", jobid, speculative ? "speculative" : "", partition, nIdx, nOldReduces);
    }
}
