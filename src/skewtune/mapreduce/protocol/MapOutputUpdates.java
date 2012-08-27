package skewtune.mapreduce.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapOutputIndex;
import org.apache.hadoop.mapred.MapOutputIndex.Record;
import org.apache.hadoop.mapred.TaskID;

/**
 * Alternative location of map output
 * @author yongchul
 */
public class MapOutputUpdates implements Writable, Iterable<NewMapOutput> {
    private NewMapOutput[] completedMaps; // map identifier
    private TaskStatusEvent.Internal[] takeoverMaps;

    public static final NewMapOutput[] EMPTY_NEW_MAP_OUTPUT = new NewMapOutput[0];
    public static final TaskStatusEvent.Internal[] EMPTY_NEW_TAKEOVER_MAP = new TaskStatusEvent.Internal[0];
    public static final MapOutputUpdates EMPTY_UPDATE = new MapOutputUpdates(EMPTY_NEW_MAP_OUTPUT,EMPTY_NEW_TAKEOVER_MAP);
    
    public MapOutputUpdates() {}

    public MapOutputUpdates(NewMapOutput[] v,TaskStatusEvent.Internal[] t) {
        completedMaps = v;
        takeoverMaps = t;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        completedMaps = new NewMapOutput[in.readInt()];
        for ( int i = 0; i < completedMaps.length; ++i ) {
            NewMapOutput newMapOutput = new NewMapOutput();
            newMapOutput.readFields(in);
            completedMaps[i] = newMapOutput;
        }
        takeoverMaps = new TaskStatusEvent.Internal[in.readInt()];
        for ( int i = 0; i < takeoverMaps.length; ++i ) {
            takeoverMaps[i] = new TaskStatusEvent.Internal();
            takeoverMaps[i].readFields(in);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(completedMaps.length);
        for ( NewMapOutput v : completedMaps ) {
            v.write(out);
        }
        out.writeInt(takeoverMaps.length);
        for ( int i = 0; i < takeoverMaps.length; ++i ) {
            takeoverMaps[i].write(out);
        }
    }
    
    public NewMapOutput[] getCompletedMaps() { return completedMaps; }
    public int size() { return completedMaps.length; }
    
    public TaskStatusEvent.Internal[] getTakeoverMaps() { return takeoverMaps; }

    @Override
    public Iterator<NewMapOutput> iterator() {
        return new Iterator<NewMapOutput>() {
            final int limit = completedMaps.length;
            int pos = 0;

            @Override
            public boolean hasNext() {
                return pos < limit;
            }

            @Override
            public NewMapOutput next() {
                return completedMaps[pos++];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }};
    }
}
