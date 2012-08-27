package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MapOutputIndex;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.reduce.MapHost.State;

import skewtune.mapreduce.protocol.NewMapOutput;

/**
 * keep track of status
 * 
 * @author yongchul
 * 
 */
public class ReactiveMapOutput<K, V> {
    private static final Log LOG = LogFactory.getLog(ReactiveMapOutput.class);

    private final TaskID taskid;

    private final MapOutputIndex index;

    private int scheduled;

    private int prepared;
    private int committed;

    private ArrayList<FetchTask<K, V>> tasks;

    private boolean allSucceeded = true;
    
    private boolean preCommitted;

    public static enum State {
        PENDING, BUSY, READY, COMMIT, ABORT
    }
    
    private final State[] taskState;

    private final boolean speculative;
    
    public ReactiveMapOutput(TaskAttemptID reduce, NewMapOutput out, Random rnd) {
        this.taskid = new TaskID(reduce.getJobID(), TaskType.MAP,
                out.getPartition());
        this.index = out.getIndex();
        this.speculative = out.isSpeculative();

        final int sz = index.size();
        tasks = new ArrayList<FetchTask<K, V>>(sz);
        taskState = new State[sz];
        for (int i = 0; i < sz; ++i) {
            tasks.add(new FetchTask<K, V>(this, i));
            taskState[i] = State.PENDING;
        }
        
        if ( ! this.speculative ) {
            // if this is take over, it could be committed as partial output is received.
            preCommitted = true;
        }
    }

    public TaskID getTaskID() {
        return taskid;
    }

    public TaskAttemptID getTaskAttemptID(int id) {
        return new TaskAttemptID(taskid, (id + 1) * -1);
    }

    public int getNumOutputs() {
        return index.size();
    }

    public long getTotalSize() {
        return index.getTotalRawLength();
    }

    public long getCompressedTotalSize() {
        return index.getTotalCompressedLength();
    }

    public MapOutputIndex.Record getIndex(int r) {
        return index.getIndex(r);
    }
    
    public synchronized boolean isAborted() {
        return ! allSucceeded;
    }
    
    public boolean isSpeculative() {
        return speculative;
    }
    
    public synchronized void markBusy(int m) {
        taskState[m] = State.BUSY;
    }
    
    public synchronized int precommit() throws IOException {
        if ( preCommitted ) {
            return 0; // nothing to do
        }

        for ( int i = 0; i < taskState.length; ++i ) {
            if ( taskState[i] == State.READY ) {
                tasks.get(i).out.commit();
                taskState[i] = State.COMMIT;
                ++committed;
            }
        }
        preCommitted = true;
        return committed;
    }

    /**
     * if return
     * 
     * @param m
     * @param out
     * @return
     */
    public synchronized boolean commit(int id) throws IOException {
        if ( preCommitted ) {
            tasks.get(id).out.commit();
            ++committed;
        } else if ( allSucceeded && prepared == tasks.size() ) {
            for ( FetchTask<K,V> task : tasks ) {
                if ( task.out == null ) {
                    throw new IOException("Mapoutput is null for fetch task "+task.getTaskAttemptID());
                }
                task.out.commit();
                ++committed;
            }
        } else {
            throw new IOException(String.format("Inconsistent state to commit %d out of %d committed",committed,tasks.size()));
        }
        return committed == tasks.size();
    }
    
    public synchronized State readyToCommit(int m) {
        State done = State.ABORT;
        if ( preCommitted ) {
            ++prepared;
            taskState[m] = State.READY;
            done = State.COMMIT;
        } else if ( allSucceeded ) {
            ++prepared;
            taskState[m] = State.READY;
            done = prepared == tasks.size() ? State.COMMIT : State.PENDING;
        } else {
            // someone has aborted me.
            FetchTask<K,V> task = tasks.get(m);
            // someone already has aborted me. my state is already ABORT
            if ( task.out != null ) {
                task.out.abort();
                task.out = null;
                taskState[m] = State.ABORT;
            }
        }
        return done;
    }

    /**
     * abort partition m. if return true, the caller is responsible to abort
     * previously committed outputs
     * 
     * @param m
     * @return true
     */
    public synchronized void abort(int m) {
        if ( preCommitted )
            throw new IllegalStateException(taskid + " has been precommitted. can't abort!");
        
        if ( allSucceeded ) {
            abortAll();
        } else {
            FetchTask<K,V> task = tasks.get(m);
            // someone already has aborted me. my state is already ABORT
            if ( task.out != null ) {
                task.out.abort();
                task.out = null;
            }
        }
    }

    /**
     * abort all
     */
    public synchronized void abortAll() {
        if ( preCommitted )
            throw new IllegalStateException(taskid + " has been precommitted. can't abort!");

        if ( allSucceeded ) {
            allSucceeded = false;
            for ( int i = 0; i < index.size(); ++i ) {
                if ( taskState[i] == State.READY ) {
                    tasks.get(i).out.abort();
                    tasks.get(i).out = null; // no longer needed.
                }
                taskState[i] = State.ABORT;
            }
        }
    }
    
    public synchronized Collection<FetchTask<K, V>> getAllTasks() {
        return tasks;
    }
}

class FetchTask<K, V> {
    final int partition;
    final ReactiveMapOutput<K, V> output;
    final MapOutputIndex.Record offset;
    int retryCount;

    volatile MapOutput<K, V> out;

    public FetchTask(ReactiveMapOutput<K, V> output, int partition) {
        this.partition = partition;
        this.output = output;
        this.offset = output.getIndex(partition);
    }

    public int getPartition() {
        return partition;
    }

    public TaskAttemptID getTaskAttemptID() {
        return output.getTaskAttemptID(partition);
    }

    public TaskID getTaskID() {
        return output.getTaskID();
    }
    
    public int getMapID() {
        return output.getTaskID().getId();
    }

    public MapOutput<K, V> getMapOutput() {
        return out;
    }
    
    public ReactiveMapOutput<K,V> getReactiveMapOutput() {
        return output;
    }

    public MapOutputIndex.Record getIndex() {
        return offset;
    }

    public void setMapOutput(MapOutput<K, V> out) {
        this.out = out;
    }
    
    public org.apache.hadoop.mapreduce.task.reduce.ReactiveMapOutput.State readyToCommit() {
        return output.readyToCommit(partition);
    }
    
    public boolean commit() throws IOException {
        return output.commit(partition);
    }
    
    public void incrRetry() {
        ++retryCount;
    }
    
    public void abort() {
        output.abort(partition);
    }
    
    public void markBusy() {
        output.markBusy(partition);
    }

    // previously it was
    // .skewreduce/m-?-?/part-0000?
    // .skewreduce/m-00000-2/part-m-00000
    // now it's output/m-<partition>-<# splits>
    public Path getInputPath(String outPath) {
        return new Path(String.format("%s/m-%05d-%d/part-%05d", outPath,
                output.getTaskID().getId(), output.getNumOutputs(), this.partition));

//        return new Path(String.format("%s/.skewreduce/m-%05d-%d/part-%05d",
//                outPath, output.getTaskID().getId(), output.getNumOutputs(),
//                this.partition));
    }

    @Override
    public String toString() {
        return String.format("%s (%d bytes)", getTaskAttemptID(),
                offset.getRawLength());
    }

    public boolean isAborted() {
        return output.isAborted();
    }
    
    public boolean isSpeculative() {
        return output.isSpeculative();
    }
}
