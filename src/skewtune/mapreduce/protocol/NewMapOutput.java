package skewtune.mapreduce.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapOutputIndex;

/**
 * between reduce task and task tracker.
 * slice the mapoutput indexes for querying reduce task
 * @author yongchul
 *
 */
public class NewMapOutput implements Writable {
    public static final MapOutputIndex.Record[] EMPTY = new MapOutputIndex.Record[0];

    int partition;
    MapOutputIndex index;
    long totalRawSize = -1;
    
    /**
     * if true, the map output is from speculative execution. thus, the output should not be mixed with the original map output.
     * if false, the map output is remaining part of the map, and should be combined with the original map output.
     */
    boolean speculative;
    
    public NewMapOutput() {
        index = new MapOutputIndex();
        speculative = true; // by default it's true
    }
    
    public NewMapOutput(ReactiveMapOutput totalOutput,int reduce) {
        this.partition = totalOutput.getPartition();
        this.index = totalOutput.getOutputIndex(reduce);
        this.speculative = totalOutput.isSpeculative();
    }
    
    public NewMapOutput(int partition,MapOutputIndex index) {
        this.partition = partition;
        this.index = index;
    }
    
    /**
     * get the original partition number
     * @return
     */
    public int getPartition() { return partition; }
    public MapOutputIndex getIndex() { return index; }
    public boolean isSpeculative() { return speculative; }
    /**
     * get the number of partitions to retrieve
     * @return
     */
    public int size() { return index.size(); }
    
    public long getRawTotalSize() {
        if ( totalRawSize < 0 ) {
            totalRawSize = index.getTotalRawLength();
        }
        return totalRawSize;
    }

    /**
     * get index of partition p
     * @param p
     * @return
     */
    public MapOutputIndex.Record getIndex(int p) {
        return index.getIndex(p);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(partition);
        index.write(out);
        out.writeBoolean(speculative);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        partition = in.readInt();
        index.readFields(in);
        speculative = in.readBoolean();
    }
}
