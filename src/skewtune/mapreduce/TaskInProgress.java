package skewtune.mapreduce;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.MapOutputIndex;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapred.JobClient.TaskStatusFilter;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;

import skewtune.mapreduce.JobInProgress.JobType;
import skewtune.mapreduce.protocol.STTaskStatus;
import skewtune.mapreduce.protocol.TaskAction;
import skewtune.utils.Average;

/**
 * TaskInProgress corresponds to org.apache.hadoop.mapred.TaskInProgress.
 * Track job dependencies, statistics, and progress.
 * 
 * @author yongchul
 *
 */
class TaskInProgress {
    private static final Log LOG = LogFactory.getLog(TaskInProgress.class.getName());
    
    final TaskID tid;
    final JobInProgress job; // job which this task belongs to.
    
    // for Map, keep track of tuple/bytes statistics per reduce.
    //   also,

    // for Reduce, what to track?
    
    // taskattempt to tracker
    TreeMap<TaskAttemptID,String> taskid2tracker = new TreeMap<TaskAttemptID,String>();
    TreeMap<TaskAttemptID,STTaskStatus> taskStatus = new TreeMap<TaskAttemptID,STTaskStatus>();
    TreeMap<TaskAttemptID,Integer> actions = new TreeMap<TaskAttemptID,Integer>();
    
    TaskAttemptID taskCommitted; // or where the output available
    volatile String committedHost;
    JobInProgress reactiveJob; // reactive job for this
    boolean reactiveJobCompleted; // reactive job has completed

    MapOutputIndex mapOutputIndex;
    
    TaskState state;
    
    // CANCEL
    volatile boolean canceled;
    
    TaskSplitMetaInfo metaSplit;
    
    // TASK
    // if handovered, then there exists recursive chain of the structure
    
    // original task -- handovered [ task1 ... task i [ handovered ] task k ]
    // speculation?
    
   
    TaskInProgress(JobInProgress jip,TaskType type,int id) {
        this.job = jip;
        this.tid = new TaskID(jip.getJobID(), type, id);
        this.state = TaskState.RUNNING;
    }
    
//    void cancel() { canceled = true; }
//    boolean hasCancelled() { return canceled; }
    void cancel() { canceled = true; }
    boolean hasCancelled() { return canceled; }
    synchronized boolean neverScheduled() { return taskStatus.isEmpty(); }
    synchronized boolean hasCommitted() { return taskCommitted != null; }
    synchronized TaskState getState() { return state; }
    synchronized TaskState setState(TaskState state) {
        TaskState tmp = this.state;
        this.state = state;
        return tmp;
    }
    
    JobInProgress getJob() { return job; }
    TaskID getTaskID() { return tid; }
    TaskType getType() { return tid.getTaskType(); }
    int getPartition() { return tid.getId(); }
    
    public synchronized boolean setStatus(STTaskStatus status, String host) {
        // FIXME update map output statistics if it has
        // FIXME update statistics
        
        TaskAttemptID taskid = status.getTaskID();
        if ( ! taskStatus.containsKey(taskid) ) {
            job.jobtracker.createTaskEntry(taskid, host, this);
            taskid2tracker.put(taskid, host);
        }
        
        
        if ( job.getJobType() == JobType.MAP_REACTIVE ) {
            setMapOutputIndex(status.getMapOutputIndex());
        }
        
        taskStatus.put(taskid, status);
        
        TaskStatus.State runState = status.getRunState();
        if ( runState == TaskStatus.State.SUCCEEDED ) {
            taskCommitted = taskid;
            committedHost = host;
            if ( LOG.isTraceEnabled() ) {
                LOG.trace("task "+taskid+ " has succeeded at host "+committedHost);
            }
            
            if ( state == TaskState.TAKEOVER || state == TaskState.WAIT_TAKEOVER ) {
                // no change
            } else if ( state == TaskState.PREPARE_TAKEOVER ) {
                state = TaskState.WAIT_TAKEOVER;
            } else {
                // RUNNING, CANCEL_TAKEOVER, COMPLETE
                state = TaskState.COMPLETE;
            }
        }
        
        if ( runState != TaskStatus.State.RUNNING ) {
            job.jobtracker.removeTaskEntry(taskid);
        }
        
        return taskCommitted != null;
    }
    
    public synchronized int getAction(TaskAttemptID attempt) {
        if ( canceled ) return TaskAction.FLAG_CANCEL;
        Integer action = actions.remove(attempt);
        return action == null ? 0 : action;
    }
    
    public synchronized void setAction(TaskAction action) {
        if ( canceled ) {
            LOG.warn("task "+action.getAttemptID()+" has already been cancelled");
        } else {
            actions.put(action.getAttemptID(), action.getAction());
        }
    }
    
    public synchronized void setMapOutputIndex(MapOutputIndex index) {
        if ( mapOutputIndex != null ) {
            if ( LOG.isTraceEnabled() ) {
                LOG.trace(tid.toString() + " already has an output index");
            }
            return;
        } else if ( index != null ) {
            mapOutputIndex = index;
            if ( LOG.isTraceEnabled() ) {
                LOG.trace(tid.toString() + " setting map output index = "+index);
            }
        }
    }
    
    public String getHost() {
        return committedHost;
    }
    
    public synchronized JobInProgress getReactiveJob() {
        return reactiveJob;
    }
    
    public synchronized void setReactiveJob(JobInProgress jip) {
        reactiveJob = jip;
        if ( jip != null && ! jip.isSpeculative() ) {
            if ( state != TaskState.PREPARE_TAKEOVER ) {
                LOG.error("Invalid task state : "+state);
            }
            state = TaskState.TAKEOVER;
        }
    }
    
    public synchronized boolean hasReactiveJob() {
        return reactiveJob != null;
    }


    public synchronized void setMetaSplit(TaskSplitMetaInfo taskSplitMetaInfo) {
        metaSplit = taskSplitMetaInfo;
    }
    
    public synchronized TaskSplitMetaInfo getMetaSplit() {
        return metaSplit;
    }
    
    public synchronized MapOutputIndex getMapOutputIndex() {
        return mapOutputIndex;
    }

    public synchronized void getAllTaskStatus(List<STTaskStatus> cache,long now) {
        if ( this.taskCommitted == null && this.reactiveJob == null ) {
            // extrac check for map
            if ( tid.getTaskType() == TaskType.MAP
                    && ( largeRecords != null && splitFactor < 2.0f ) ) {
                // if there is at least one record and split factor is less than 2,
                // there is no point to further split this task
                return;
            }
            
            if ( state == TaskState.PREPARE_TAKEOVER
                    || state == TaskState.WAIT_TAKEOVER
                    || state == TaskState.TAKEOVER ) {
                return; // takeover in progress
            }
            
            // either reduce or has not set, otherwise, at least splittable more than two
            STTaskStatus best = null;
            double bestTime = Double.MAX_VALUE;
            for ( STTaskStatus status : taskStatus.values() ) {
                if ( status.getTimePassed(now) > 60.
                        && status.getRemainTimeAt() > 0
                        && status.getRunState() == TaskStatus.State.RUNNING ) {
//                    double est = status.getEstimateTime(now);
                    double est = status.getRemainTime(now);
                    if ( est < bestTime ) {
                        bestTime = est;
                        best = status;
                    }
                }
            }
            
            if ( best != null ) {
                if ( bestTime > 60.0 ) {
                    cache.add(best);
                }
//                } else {
//                    if ( LOG.isTraceEnabled() ) {
//                        LOG.trace("reject "+best.getTaskID()+" remain time = "+bestTime+" at "+best.getProgress()+"@"+best.getPhase());
//                    }
//                }
            }
        }
    }

    /**
     * copied from org.apache.hadoop.mapred.MapTask
     * @param <T>
     * @param file
     * @param offset
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private <T> T getSplitDetails(Path file, long offset) throws IOException {
        Configuration conf = job.getConfiguration();
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream inFile = fs.open(file);
        inFile.seek(offset);
        String className = Text.readString(inFile);
        Class<T> cls;
        try {
            cls = (Class<T>) conf.getClassByName(className);
        } catch (ClassNotFoundException ce) {
            IOException wrap = new IOException("Split class " + className + " not found");
            wrap.initCause(ce);
            throw wrap;
        }
        SerializationFactory factory = new SerializationFactory(conf);
        Deserializer<T> deserializer = (Deserializer<T>) factory
                .getDeserializer(cls);
        deserializer.open(inFile);
        T split = deserializer.deserialize(null);
        inFile.close();
        return split;
    }
    
    public <T> T getInputSplit() throws IOException {
        // FIXME convert to appropriate type
        TaskSplitIndex splitIndex = metaSplit.getSplitIndex();
        return getSplitDetails(new Path(splitIndex.getSplitLocation()),splitIndex.getStartOffset());
    }
    
    // FIXME only for probing map input
    public org.apache.hadoop.mapreduce.lib.input.FileSplit getNewFileInputSplit() throws IOException {
        Object o = getInputSplit();
        if ( o instanceof org.apache.hadoop.mapred.FileSplit ) {
            org.apache.hadoop.mapred.FileSplit split = (org.apache.hadoop.mapred.FileSplit)o;
            return new org.apache.hadoop.mapreduce.lib.input.FileSplit(split.getPath(),split.getStart(),split.getLength(),split.getLocations());
        }
        return (org.apache.hadoop.mapreduce.lib.input.FileSplit)o;
    }

    public TaskType getTaskType() {
        return tid.getTaskType();
    }


    float splitFactor = Float.MAX_VALUE;
    TreeSet<Record> largeRecords;
    
    public synchronized boolean addLargeRecord(float factor,long offset, int len) {
        if ( largeRecords == null )
            largeRecords = new TreeSet<Record>();
        largeRecords.add(new Record(offset,len));
        splitFactor = Math.min(splitFactor,factor);
        return reactiveJob != null && reactiveJob.getNumMapTasks() > splitFactor;
    }
    
    public synchronized int adjustNumSplits(int n) {
        if ( tid.getTaskType() == TaskType.MAP ) {
//            if ( job.hasCombiner() ) {
//                return Math.min((int)splitFactor,3);
//            } else 
            if ( splitFactor != Float.MAX_VALUE ){
                return (int)splitFactor; // we floor the value to make sure that the large record does not go beyond tuple boundary
            }
        }
        return n;
    }
    
    public static class Record implements Comparable<Record> {
        final long offset;
        final int len;
        
        public Record(long offset,int len) {
            this.offset = offset;
            this.len = len;
        }
        
        public int length() { return len; }
        public long offset() { return offset; }
        
        @Override
        public int hashCode() { return (int)offset; }
        
        @Override
        public boolean equals(Object o) {
            if ( this == o ) return true;
            if ( o instanceof Record ) {
                return offset == ((Record)o).offset && len == ((Record)o).len;
            }
            return false;
        }
        
        @Override
        public String toString() {
            return String.format("[%d,%d)",offset,offset+len);
        }

        @Override
        public int compareTo(Record o) {
            return Long.signum(offset - o.offset);
        }
    }
    
    /**
     * @return <code>null</code> if only speculative execution makes sense.
     */
    public synchronized TaskAttemptWithHost getSplittableTask() {
        STTaskStatus best = null;
        float bestProgress = Float.MIN_VALUE;
        TaskAttemptWithHost ret = null;
        
        if ( state == TaskState.RUNNING ) {
            if ( tid.getTaskType() == TaskType.MAP ) {
                // taskattempts which is in MAP phase and the most progressed one
                for ( STTaskStatus status : taskStatus.values() ) {
                    if ( status.getPhase() == Phase.MAP && status.getProgress() > bestProgress ) {
                        best = status;
                    }
                }
            } else {
                int bestPhase = -1;
                // taskattempts which already started reduce
                for ( STTaskStatus status : taskStatus.values() ) {
                    // reduce can stop anywhere.
                    // come up with best by comparing phase and progress
                    /*
                    if ( status.getPhase() == Phase.REDUCE && status.getProgress() > bestProgress ) {
                        best = status;
                    }
                    */
                    int phase = status.getPhase().ordinal();
                    float progress = status.getProgress();
                    if ( phase < bestPhase ) {
                        continue; // don't even think about it
                    } else if ( phase > bestPhase ) {
                        bestPhase = phase;
                        bestProgress = progress;
                        best = status;
                    } else if ( progress > bestProgress ) {
                        bestProgress = progress;
                        best = status;
                    }
                }
            }
            
            if ( best != null ) {
                TaskAttemptID id = best.getTaskID();
                ret = new TaskAttemptWithHost(id,taskid2tracker.get(id));
            }
        }

        return ret;
    }
    
    public synchronized boolean hasReactiveJobCompleted() {
        return this.reactiveJobCompleted;
    }
    public synchronized void setReactiveJobCompleted() {
        this.reactiveJobCompleted = true;
    }
    
    public synchronized double getRemainingTime(TaskAttemptID taskid,long now) {
        STTaskStatus status = taskStatus.get(taskid);
        if ( status == null || status.getRunState() != TaskStatus.State.RUNNING ) {
            return 0.; // assuming completed?
        }
        return Math.max(status.getRemainTime(now),0.);
    }
    public synchronized void getTimePerByte(Average avg) {
        for ( STTaskStatus status : taskStatus.values() ) {
            float v = status.getTimePerByte();
            if ( v > 0. ) avg.add(v);
        }
    }
}
