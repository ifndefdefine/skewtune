package skewtune.mapreduce;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapOutputIndex;
import org.apache.hadoop.mapred.RelativeOffset;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.security.TokenStorage;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.PartitionMapInput.MapInputPartition;
import skewtune.mapreduce.PartitionMapOutput.MapOutputPartition;
import skewtune.mapreduce.PartitionPlanner.Partition;
import skewtune.mapreduce.PartitionPlanner.Plan;
import skewtune.mapreduce.PartitionPlanner.PlanSpec;
import skewtune.mapreduce.PartitionPlanner.PlanType;
import skewtune.mapreduce.PartitionPlanner.ReactiveMapPlan;
import skewtune.mapreduce.PartitionPlanner.ReactiveReducePlan;
import skewtune.mapreduce.SimpleCostModel.Estimator;
import skewtune.mapreduce.lib.input.DelegateInputFormat;
import skewtune.mapreduce.lib.input.InputSplitCache;
import skewtune.mapreduce.lib.input.MapOutputInputFormat;
import skewtune.mapreduce.lib.input.MapOutputSplit;
import skewtune.mapreduce.lib.input.MapOutputSplit.ReactiveOutputSplit;
import skewtune.mapreduce.protocol.ReactiveMapOutput;
import skewtune.mapreduce.protocol.STTaskStatus;
import skewtune.mapreduce.protocol.TaskStatusEvent;
import skewtune.mapreduce.util.PrimeTable;
import skewtune.utils.Average;
import skewtune.utils.Base64;
import skewtune.utils.Utils;

/**
 * JobInProgress corresponds to org.apache.hadoop.mapred.JobInProgress. Track
 * job dependencies, statistics, and progress.
 * 
 * @author yongchul
 * 
 */
class JobInProgress implements MRJobConfig, SkewTuneJobConfig {
    public static final Log LOG = LogFactory.getLog(JobInProgress.class);

    static enum JobType {
        ORIGINAL, MAP_REACTIVE, REDUCE_REACTIVE, REDUCE_PLAN
    }

    static enum State {
        STARTING, RUNNING, SUCCEEDED, KILLED, FAILED
    }

    final STJobTracker jobtracker;
    final JobType type;
    final JobInProgress parent;
    final int partition; // if reactive job, capture original partition number
    final Cluster cluster; // cluster which the job is running on
    final Job job; // job instance backing this object
    final JobConf confTemplate; // only assigned for original job
    final boolean speculative; // if reactive, is it speculative job or acceleration job?
    final boolean takeover; // if true, takeover is enabled.
    final Plan plan; // if this is a mitigation job, save associated plan

    final boolean doNotSpeculate; // FOR EXPERIMENT

    volatile State runState = State.STARTING;
    volatile boolean localized;

    TokenStorage tokenStorage;
    final UserGroupInformation ugi;

    private TreeMap<JobID, JobInProgress> reactiveJobs = new TreeMap<JobID, JobInProgress>();
    private TreeMap<Integer, JobInProgress> reactiveMaps = new TreeMap<Integer, JobInProgress>();
    private TreeMap<Integer, JobInProgress> reactiveReduces = new TreeMap<Integer, JobInProgress>();

    static class MapOutputAndHost {
        final String[] hosts;

        final ReactiveMapOutput mapOutputs;

        MapOutputAndHost(ReactiveMapOutput output, String[] hosts) {
            this.mapOutputs = output;
            this.hosts = hosts;
        }

        public ReactiveMapOutput getMapOutput() {
            return mapOutputs;
        }
        
        public int getNumFiles() {
            return mapOutputs.getNumIndexes();
        }

        public String[] getHosts() {
            return hosts;
        }

        public int getPartition() {
            return mapOutputs.getPartition();
        }
    }

    // newly available map output
    // private ReactiveMapOutput[] newMapOutput;
    private MapOutputAndHost[] newMapOutput;
    private List<TaskStatusEvent> newTakeOver;

    private AvailableMapOutput availMapOutput;

    private BitSet completedReactiveJobs;
    private BitSet completedTasks; // fully completed
    private int numCompletedMaps;
    
    // for reactive map job, set available map output index
    private BitSet availMapOutIndex;

    class AvailableMapOutput {
        final int nMaps;
        BitSet original;
        BitSet reactive;
        BitSet testBuf;
        
        BitSet takeover;
        BitSet testTakeOverBuf;

        AvailableMapOutput(int nMaps) {
            original = new BitSet(nMaps);
            reactive = new BitSet(nMaps);
            testBuf = new BitSet(nMaps);
            
            takeover = new BitSet(nMaps);
            testTakeOverBuf = new BitSet(nMaps);
            
            this.nMaps = nMaps;
        }
        
        /**
         * check whether there exist running take over reactive job
         * @return <code>true</code> if there is any running take over job.
         */
        private boolean checkTakeOver() {
            if ( takeover.isEmpty() ) {
                return false;
            } else {
                testTakeOverBuf.clear();
                testTakeOverBuf.or(original);
                testTakeOverBuf.and(reactive);
                testTakeOverBuf.and(takeover);
            }
            
            return ! testTakeOverBuf.equals(takeover);
        }
        
        private boolean checkAllAvailable() {
            return ! checkTakeOver() && testBuf.cardinality() == nMaps;
        }
        
        synchronized int getCompletedOriginal() {
            return original.cardinality();
        }
        
        synchronized boolean allAvailable(boolean wait) throws InterruptedException {
            if (wait) {
                while ( runState == State.RUNNING ) {
                    if ( checkAllAvailable() ) {
                        return true;
                    } else {
                        wait(1000); // check every second
                    }
                }
                if ( LOG.isDebugEnabled() ) {
                    LOG.debug(getJobID() + " has been "+runState);
                }
                throw new InterruptedException();
            }
//            return testBuf.cardinality() == nMaps;
            return checkAllAvailable();
        }

        synchronized boolean updateOriginalTask(BitSet set) {
            original.or(set);
            testBuf.or(set);
            if ( checkAllAvailable() ) {
                notifyAll();
                return true;
            } else {
                if ( LOG.isTraceEnabled() ) {
                    LOG.trace("available outputs = "+testBuf);
                }
            }
            return false;
        }

        synchronized boolean updateReactiveTask(int map) {
            reactive.set(map);
            testBuf.set(map);
            if ( checkAllAvailable() ) {
                notifyAll();
                return true;
            } else {
                if ( LOG.isTraceEnabled() ) {
                    LOG.trace("available outputs = "+testBuf);
                }
            }
            return false;
        }
        
        synchronized void setTakeOver(int map) {
            takeover.set(map);
        }
        synchronized void unsetTakeOver(int map) {
            takeover.clear(map);
        }

        synchronized BitSet getOriginal() {
            return (BitSet) original.clone();
        }

        synchronized BitSet getReactive() {
            return (BitSet) reactive.clone();
        }

        synchronized BitSet onlyInReactive() {
            BitSet copy = (BitSet) reactive.clone();
            copy.andNot(original);
            if ( ! takeover.isEmpty() ) {
                BitSet takeoverCopy = (BitSet) takeover.clone();
                takeoverCopy.and(reactive);
                copy.or(takeoverCopy);
            }
            return copy;
        }
        
        synchronized boolean require(int p) {
            int avail = testBuf.cardinality();
            if ( avail == nMaps ) return false;
            int needed = nMaps - avail;
            return needed < 5 && ! testBuf.get(p);
        }
    }

    private int numMapOutput;
    private int numMaps;
    private int numReduces;
    private int numScheduledMaps;
    private int numScheduledReduces;

    private TaskInProgress[] tasks; // task in progresses. map + reduce

    // track average time per byte for map and reduce. only track completed tasks.
    private Average mapAvgTimePerByte = new Average();
    private Average reduceAvgTimePerByte = new Average();
    
    private Set<Path> pathToDelete = new HashSet<Path>();

    /**
     * constructor for reactive job. many other fields are initialized once we
     * got a job id
     * 
     * @param cluster
     * @param type
     * @param parent
     * @param partition
     * @param id
     * @param conf
     * @param ugi
     * @throws IOException
     */
    private JobInProgress(JobType type, JobInProgress parent,
            int partition, boolean speculative, Plan plan) throws IOException {
        this.jobtracker = parent.jobtracker;
        this.type = type;
        this.parent = parent;
        this.partition = partition;
        this.cluster = jobtracker.getCluster();
        this.confTemplate = null;
        this.job = Job.getInstance(this.cluster, parent.getConfTemplate());
        this.ugi = parent.getUGI();
        this.speculative = speculative;
//        this.takeover = false;
        this.takeover = parent.canTakeover();
        this.newTakeOver = Collections.emptyList();
        this.doNotSpeculate = false;
        this.plan = plan;
        
        if ( type == JobType.REDUCE_REACTIVE ) {
            this.completedMapEvents = new ArrayList<TaskCompletionEvent>(numMaps+10);
            this.completedReactiveJobs = new BitSet(numMaps + numReduces);
        } else {
            this.completedMapEvents = Collections.emptyList();
        }
    }

    public JobInProgress(STJobTracker jobtracker, Job runningJob, Configuration conf) {
        this.jobtracker = jobtracker;
        this.type = JobType.ORIGINAL;
        this.parent = null;
        this.partition = 1;
        this.cluster = jobtracker.getCluster();
        this.job = runningJob;
        this.confTemplate = new JobConf(conf);
        this.ugi = UserGroupInformation.createRemoteUser(conf.get(USER_NAME));
        this.speculative = false;
        this.takeover = conf.getBoolean(SkewTuneJobConfig.ENABLE_SKEWTUNE_TAKEOVER, false);
        this.doNotSpeculate = conf.getBoolean(SkewTuneJobConfig.SKEWTUNE_DO_NOT_SPECULATE, false);
        this.plan = null;
        
        initialize();

        this.newMapOutput = new MapOutputAndHost[numMaps];
        this.completedMapEvents = new ArrayList<TaskCompletionEvent>(numMaps + 10);
        this.completedReactiveJobs = new BitSet(numMaps + numReduces);
        // this.completedTasks = new BitSet(numMaps + numReduces);
        this.availMapOutput = new AvailableMapOutput(numMaps);
        this.newTakeOver = new ArrayList<TaskStatusEvent>();
    }

    public synchronized void initialize() {
        // jobid is already set
        numMaps = getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
        numReduces = getConfiguration().getInt(MRJobConfig.NUM_REDUCES, 1);

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("%s -- # of maps = %d; # of reduces = %d",
                    getJobID(), numMaps, numReduces));
        }

        tasks = new TaskInProgress[numMaps + numReduces];
        int i;
        for (i = 0; i < numMaps; ++i)
            tasks[i] = new TaskInProgress(this, TaskType.MAP, i);
        for (int j = 0; i < tasks.length; ++i, ++j)
            tasks[i] = new TaskInProgress(this, TaskType.REDUCE, j);
        
        completedTasks = new BitSet(numMaps + numReduces);
        runState = State.RUNNING;
    }
    
    void initializeReactiveMap(int n) {
        if ( type != JobType.MAP_REACTIVE )
            throw new IllegalStateException("invalid job type = "+type);
        this.availMapOutIndex = new BitSet(n);
        this.numMaps = n;
    }

    public JobInProgress getOriginalJob() {
        JobInProgress org = this;
        while (org.parent != null)
            org = org.parent;
        return org;
    }

    public JobConf getConfTemplate() {
        return getOriginalJob().confTemplate;
    }

    public Configuration getConfiguration() {
        return confTemplate == null ? job.getConfiguration() : confTemplate;
    }

    public int getPartition() {
        return partition;
    }

    public JobInProgress getParentJob() {
        return parent;
    }

    public TaskID getParentTaskID() {
        return new TaskID(parent.getJobID(), type == JobType.MAP_REACTIVE,partition);
    }
    
    public TaskInProgress getOriginalTaskInProgress() {
        return parent.getTaskInProgress(getParentTaskID());
    }

    public TokenStorage getTokenStorage() {
        return getTokenStorage(false);
    }
    
    public State getState() {
        return runState;
    }
    
    public State setState(State s) {
        State old = runState;
        runState = s;
        return old;
    }

    public synchronized TokenStorage getTokenStorage(boolean wait) {
        if (tokenStorage == null && wait) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("waiting for token storage load " + job.getJobID());
            }
            while (tokenStorage == null) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    LOG.error("interrupted while waiting for token storage load",e);
                    break;
                }
            }
        }
        return tokenStorage;
    }

    synchronized void setTokenStorage(TokenStorage storage) {
        tokenStorage = storage;
        notifyAll();
    }

    public void setLocalized() {
        localized = true;
    }

    // kill all the jobs derived from this one

    public void kill() throws IOException, InterruptedException {
        if (!job.isComplete()) {
            job.killJob();
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Job getJob() {
        return job;
    }

    public JobID getJobID() {
        return job.getJobID();
    }

    public org.apache.hadoop.mapred.JobID getDowngradeJobID() {
        return org.apache.hadoop.mapred.JobID.downgrade(job.getJobID());
    }

    public String getUser() {
        return job.getUser();
    }

    public UserGroupInformation getUGI() {
        return ugi;
    }

    JobType getJobType() {
        return type;
    }
    
    public boolean isSpeculative() {
        return speculative;
    }
    
    public boolean canTakeover() {
        return takeover;
    }

    public Path getJobDir() {
        // FIXME it may change in future?
        return new Path(getConfiguration().get("mapreduce.job.dir"));
    }

    public synchronized void addReactiveJob(JobInProgress subjob) {
        reactiveJobs.put(subjob.getJobID(), subjob);
        pathToDelete.remove(subjob.getOutputPath());
    }

    public int getNumMapTasks() {
        return numMaps;
    }

    public int getNumReduceTasks() {
        return numReduces;
    }

    public Path getOutputPath() {
        // FIXME hard coded
        String p = getConfiguration().get(FileOutputFormat.OUTDIR);
        return p == null ? null : new Path(p);
    }

    public void kill(boolean recurse, Map<org.apache.hadoop.mapred.JobID, JobInProgress> subjobs) throws IOException, InterruptedException {
        job.killJob();
        synchronized (subjobs) {
            subjobs.remove(getJobID());
        }
        
        if (recurse) {
            for (JobInProgress subjob : reactiveJobs.values()) {
                subjob.kill(recurse, subjobs);
            }
        }
    }

    private synchronized int addNewMapOutput(ReactiveMapOutput output,
            String[] hosts, boolean forceCancel) {
        newMapOutput[numMapOutput++] = new MapOutputAndHost(output, hosts);
        if (forceCancel) {
            tasks[output.getPartition()].cancel(); // cancel execution
        }
        completedReactiveJobs.set(output.getPartition());
        
        // set completed if it is speculative or handover and previous task is already committed.
        if ( this.isSpeculative() || tasks[output.getPartition()].hasCommitted() ) {
            completedTasks.set(output.getPartition());
        }
        // if ( isAllMapOutputAvailable() ) {
        // synchronized ( completedMapEvents ) {
        // completedMapEvents.notifyAll();
        // }
        // }
        if ( availMapOutput.updateReactiveTask(output.getPartition()) ) {
            LOG.info("all map outputs are available for "+job.getJobID());
        }
        return numMapOutput;
    }

    int addNewMapOutput(int partition, boolean forceCancel) {
        JobInProgress reactiveJob = tasks[partition].getReactiveJob();
        ReactiveMapOutput output = reactiveJob.getReactiveMapOutput();
        String[] hosts = reactiveJob.getCommittedHosts();

        if (LOG.isDebugEnabled()) {
            LOG.debug(output);
        }
        return addNewMapOutput(output, hosts, forceCancel);
    }

    synchronized void cancelTask(TaskType type, int partition) {
        int index = type == TaskType.MAP ? partition : partition + numMaps;
        tasks[index].cancel();
        if (type == TaskType.REDUCE) {
            completedReactiveJobs.set(index);
            if ( this.isSpeculative() || tasks[index].hasCommitted() ) {
                completedTasks.set(index);
            }
            
            // if this is the last one, cancel all original tasks
            if ( isCompleted() && availMapOutput != null ) {
                // if there are map tasks that are still running, cancel.
                // FIXME
                BitSet bs = availMapOutput.getOriginal();
                for (int i = bs.nextClearBit(0); i < tasks.length; i = bs.nextClearBit(i+1)) {
                    tasks[i].cancel();
                }    
            }
        }
    }

    /**
     * mark given task as cancel. only used when we do handover reduce task.
     * @param type
     * @param partition
     */
    synchronized void markCancel(TaskID taskid) {
        final int partition = taskid.getId();
        int index = taskid.getTaskType() == TaskType.MAP ? partition : partition + numMaps;
        tasks[index].cancel();
    }
    
    synchronized void retrieveNewMapOutput(List<ReactiveMapOutput> output,
            int from) {
        if (LOG.isTraceEnabled() && (from < numMapOutput)) {
            LOG.trace("Retrieving new map output from = " + from + " to "
                    + numMapOutput);
        }
        if (from < numMapOutput) {
            // we got something
            for (int i = from; i < numMapOutput; ++i) {
                output.add(newMapOutput[i].getMapOutput());
            }
        }
    }
    

    synchronized void retrieveNewTakeOver(ArrayList<TaskStatusEvent> output,int from) {
        if (LOG.isTraceEnabled() && (from < newTakeOver.size())) {
            LOG.trace("Retrieving new take over map output from = " + from + " to " + newTakeOver.size());
        }
        if (from < newTakeOver.size() ) {
            // we got something
            for (int i = from; i < newTakeOver.size(); ++i) {
//                output.add(new TaskID(job.getJobID(),TaskType.MAP,newTakeOver.get(i)));
                output.add(newTakeOver.get(i));
            }
        }
    }

    public boolean hasCombiner() {
        Configuration conf = getConfiguration();
        return conf.get(COMBINE_CLASS_ATTR) != null || conf.get("mapred.combiner.class") != null;
    }

    /**
     * collect all MapOutputIndex objects from the tasks and create reactive
     * mapoutput only called once this reactive job completes.
     * 
     * @return
     */
    ReactiveMapOutput getReactiveMapOutput() {
        ReactiveMapOutput r = new ReactiveMapOutput(getParentTaskID(), numMaps, parent.numReduces, speculative);
        for (int i = 0; i < numMaps; ++i) {
            r.setMapOutputIndex(i, tasks[i].getMapOutputIndex());
        }
        return r;
    }

    String[] getCommittedHosts() {
        String[] hosts = new String[numMaps + numReduces];
        for (int i = 0; i < tasks.length; ++i) {
            hosts[i] = tasks[i].getHost();
        }
        return hosts;
    }

    TaskInProgress getTaskInProgress(TaskAttemptID taskid) {
        return getTaskInProgress(taskid.getTaskID());
    }

    TaskInProgress getTaskInProgress(TaskID taskid) {
        assert taskid.getJobID().equals(job.getJobID());
        return tasks[taskid.getTaskType() == TaskType.MAP ? taskid.getId() : taskid.getId() + numMaps];
    }

    TaskInProgress getTaskInProgress(TaskType type, int partition) {
        return tasks[(type == TaskType.MAP ? 0 : numMaps) + partition];
    }

    public void notifyParentJob(boolean cancelOriginal) {
        if (parent != null) {
            TaskInProgress tip = this.getOriginalTaskInProgress();
            tip.setReactiveJobCompleted();
            
            if (type == JobType.MAP_REACTIVE) {
                // append new map output
                
                // FIXME if we recursively repartition, this map output should be added
                // all the way up to the root job
                parent.addNewMapOutput(getPartition(), cancelOriginal); // I'm
                                                                        // DONE!
            } else {
                // mark reduce task as cancelled
                parent.cancelTask(TaskType.REDUCE, getPartition());
            }
        } else {
            LOG.error("Why am I an orphan?! " + getJobID());
        }
    }

    // repartition?

    /**
     * copied from org.apache.hadoop.mapreduce.security.TokenCache
     */
    private static final Text JOB_TOKEN = new Text("ShuffleAndJobToken");

    private static String getNewJobName(String jobName, TaskID attemptid) {
        
        return String.format("%s-%s-%05d", jobName,
                attemptid.getTaskType() == TaskType.MAP ? "m" : "r", attemptid
                        .getId());
    }
    
    public TaskAttemptWithHost getSplittableTask(TaskID taskid) {
        boolean enabled = taskid.getTaskType() == TaskType.MAP 
                ? getConfiguration().getBoolean(ENABLE_SKEWTUNE_TAKEOVER_MAP, takeover)
                : getConfiguration().getBoolean(ENABLE_SKEWTUNE_TAKEOVER_REDUCE, takeover);
        if ( enabled ) {
            TaskInProgress tip = getTaskInProgress(taskid);
            return tip.getSplittableTask();
        }
        return null;
    }
    
    public static abstract class ReactionContext {
        protected final TaskInProgress tip; // target task in progress
        protected final TaskAttemptID attemptId;
        protected float timePerByte;
        protected long remainBytes;
        protected PlanSpec planSpec;
        protected Plan plan;
        protected Estimator estimator;
        protected List<Partition> partitions;
        
        protected ReactionContext(TaskInProgress tip) {
            this(tip, PartitionPlanner.getPlanSpec(tip));
        }
        
        protected ReactionContext(TaskInProgress tip,TaskAttemptID attempt) {
            this(tip, attempt, PartitionPlanner.getPlanSpec(tip));
        }
        
        protected ReactionContext(TaskInProgress tip,PlanSpec spec) {
            this.tip = tip;
            this.attemptId = null;
            this.planSpec = spec;
            this.partitions = Collections.emptyList();
        }
        
        protected ReactionContext(TaskInProgress tip,TaskAttemptID attempt,PlanSpec spec) {
            this.tip = tip;
            this.attemptId = attempt;
            this.planSpec = spec;
        }
        
        public TaskID getTaskID() { return tip.getTaskID(); }
        public TaskType getTaskType() { return tip.getTaskType(); }
        public TaskAttemptID getTargetAttemptID() { return attemptId; }
        public JobInProgress getJob() { return tip.getJob(); }
        public TaskInProgress getTaskInProgress() { return tip; }
        public Configuration getConfiguration() { return getJob().getConfiguration(); }
        public int getNumMapTasks() { return getJob().getNumMapTasks(); }
        public int getNumReduceTasks() { return getJob().getNumReduceTasks(); }
        public float getTimePerByte() { return timePerByte; }
        public long getRemainBytes() { return remainBytes; }
        /**
         * if scan task is available, update the information
         * @param bytes
         */
        public void setRemainBytes(long bytes) { remainBytes = bytes; }
        public void setTimePerByte(float tpb) { timePerByte = tpb; }
        public void setPlan(Plan p) { plan = p; }
        public Plan getPlan() { return plan; }
        public PlanSpec getPlanSpec() { return planSpec; }
        public PlanType getPlanType() {
            return planSpec.getType();
        }
        
        public String getMapClass() {
            Configuration conf = getConfiguration();
            boolean useNewApi = conf.getBoolean("mapred.mapper.new-api", false);
            return useNewApi ? conf.get(JobContext.MAP_CLASS_ATTR) : conf.get("mapred.mapper.class");
        }
        
        public String getReduceClass() {
            Configuration conf = getConfiguration();
            boolean useNewApi = conf.getBoolean("mapred.reducer.new-api", false);
            return useNewApi ? conf.get(JobContext.REDUCE_CLASS_ATTR) : conf.get("mapred.reducer.class");
        }
        
        public void setEstimator(Estimator estimator) { this.estimator = estimator; }
        public Estimator getEstimator() { return estimator; }
        public List<Partition> getPartitions() { return partitions; }

//        public void setPlanSpec(PlanSpec spec);
//        public PlanSpec getPlanSpec();
//        public Plan getPlan();
//        public void setPlan(Plan plan);
        
//        public void run(JobInProgress jip) throws IOException;
//        public void run(Configuration conf) throws IOException;

        /**
         * context defines following
         * - file split for remaining data
         * - the reduce key that remaining data is greater than
         * @return
         */
        public abstract <T> T getContext() throws IOException;
        
        /**
         * initialize scan task
         * @param newJob
         * @param job
         */
        public abstract void initScanTask(Job job) throws IOException;
        
        /**
         * initialize reactive task
         * @param newJob
         * @param job
         * @throws IOException 
         */
        public abstract void initReactiveTask(JobInProgress newJob,Job job) throws IOException;
    }
    
    public static class ReexecMap extends ReactionContext {
        final int numSplits;
        FileSplit orgSplit;
        
        public ReexecMap(TaskInProgress tip,int n) throws IOException {
            super(tip,PartitionPlanner.getPlanSpec(PlanType.MAXSLOTS));
            this.numSplits = n;
            org.apache.hadoop.mapreduce.InputSplit split = tip.getInputSplit();
            if ( split instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit ) {
                org.apache.hadoop.mapreduce.lib.input.FileSplit xSplit = (org.apache.hadoop.mapreduce.lib.input.FileSplit)split;
                orgSplit = new FileSplit(xSplit.getPath(),xSplit.getStart(),xSplit.getLength(),xSplit.getLocations());
            } else {
                orgSplit = (FileSplit)split;
            }
            remainBytes = orgSplit.getLength();
        }
        
        public ReexecMap(TaskInProgress tip,TaskAttemptID attempt,int n,float tpb,long bytes) throws IOException {
            super(tip,attempt,PartitionPlanner.getPlanSpec(PlanType.REHASH));
            this.numSplits = n;
            org.apache.hadoop.mapreduce.InputSplit split = tip.getInputSplit();
            if ( split instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit ) {
                org.apache.hadoop.mapreduce.lib.input.FileSplit xSplit = (org.apache.hadoop.mapreduce.lib.input.FileSplit)split;
                orgSplit = new FileSplit(xSplit.getPath(),xSplit.getStart(),xSplit.getLength(),xSplit.getLocations());
            } else {
                orgSplit = (FileSplit)split;
            }
            timePerByte = tpb;
            remainBytes = bytes;
        }
        
//        @Override
//        public void run(JobInProgress jip) throws IOException {
//            Configuration conf = jip.getConfiguration();
//            boolean useNewApi = conf.getBoolean("mapred.mapper.new-api", false);
//            TaskInProgress tip = jip.getOriginalTaskInProgress();
//
//            if ( useNewApi ) {
//                org.apache.hadoop.mapreduce.InputSplit split = tip.getInputSplit();
//                List<org.apache.hadoop.mapreduce.InputSplit> subSplits = InputSplitPartitioner.split(conf, split, numSplits);
//                InputSplitCache.set(jip.getConfiguration(), subSplits);
//                
//                if (LOG.isDebugEnabled()) {
//                    LOG.debug(subSplits);
//                }
//                
//                jip.initializeReactiveMap(subSplits.size());
//            } else {
//                org.apache.hadoop.mapred.InputSplit split = tip.getInputSplit();
//                List<org.apache.hadoop.mapred.InputSplit> subSplits = InputSplitPartitioner.split(conf, split, numSplits);
//                InputSplitCache.set(jip.getConfiguration(), subSplits);
//                
//                if (LOG.isDebugEnabled()) {
//                    LOG.debug(subSplits);
//                }
//                
//                jip.initializeReactiveMap(subSplits.size());
//            }
//        }

        @Override
        public <T> T getContext() throws IOException {
            return (T)orgSplit;
        }

        @Override
        public void initScanTask(Job job) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initReactiveTask(JobInProgress newJob, Job job)
                throws IOException {
            Configuration conf = job.getConfiguration();
            ReactiveMapPlan mapPlan = (ReactiveMapPlan)getPlan();
            List<?> splits = mapPlan.getSplits();
            InputSplitCache.set(conf, splits);
            newJob.initializeReactiveMap(splits.size());
        }
    }
    
    public static class ReexecReduce extends ReactionContext {
        public ReexecReduce(TaskInProgress tip) {
            super(tip, PartitionPlanner.getPlanSpec(PlanType.MAXSLOTS));
        }

        public ReexecReduce(TaskInProgress tip,TaskAttemptID attempt,float tpb,long bytes) {
            super(tip, attempt, PartitionPlanner.getPlanSpec(PlanType.REHASH));
            this.timePerByte = tpb;
            this.remainBytes = bytes;
        }

        @Override
        public <T> T getContext() throws IOException {
            return null;
        }

        @Override
        public void initScanTask(Job job) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void initReactiveTask(JobInProgress newJob, Job job)
                throws IOException {
            ReactiveReducePlan reducePlan = (ReactiveReducePlan)getPlan();
            Configuration conf = job.getConfiguration();
            boolean newApi = conf.getBoolean("mapred.mapper.new-api",false);
            String partClass = reducePlan.getPartitionClassName(newApi);
            LOG.debug("setting partitioner class to "+partClass);
            
            if ( newApi ) {
                conf.set(JobContext.PARTITIONER_CLASS_ATTR, partClass);
            } else {
                conf.set("mapred.partitioner.class", partClass);
            }
            
            DataOutputBuffer buffer = new DataOutputBuffer(8192);

            Class<?> codecClass = conf.getClass(JobContext.MAP_OUTPUT_COMPRESS_CODEC,DefaultCodec.class);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            DataOutputStream output = new DataOutputStream(codec.createOutputStream(buffer));
            reducePlan.write(output);
            output.close();
            
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("compressed partition information = "+buffer.getLength());
            }
            
            String enc = Base64.encodeToString(buffer.getData(),0,buffer.getLength(),false);
//            String enc = Base64.encode(buffer.getData(), buffer.getLength());
            conf.set(SKEWTUNE_REPARTITION_BUCKETS, enc);
            
        	if ( plan.getScheduleOrder() != null ) {
	        	int[] order = plan.getScheduleOrder();
	        	buffer.reset();
	        	
	            output = new DataOutputStream(codec.createOutputStream(buffer));
	            for ( int i : order ) {
	            	WritableUtils.writeVInt(output, i);
	            }
	            output.close();
	            
	            if ( LOG.isDebugEnabled() ) {
	                LOG.debug("compressed schedule information = "+buffer.getLength());
	            }
	            
	            enc = Base64.encodeToString(buffer.getData(),0,buffer.getLength(),false);
	            conf.set(SKEWTUNE_REPARTITION_SCHEDULES, enc);            
        	}
        }
    }
    
    public static class TakeOverMap extends ReactionContext {
        final FileSplit remainSplit;
        final int numSplits;
        final RelativeOffset ro;
        
        public TakeOverMap(TaskInProgress tip,TaskAttemptID attempt,byte[] raw,int n,float tpb, long remainBytes,int code) throws IOException {
            super(tip,attempt);
            
            DataInputBuffer buf = new DataInputBuffer();
            buf.reset(raw,raw.length);

            /* OLD
            if ( code == 0 ) {
                String path = Text.readString(buf);
                long pos = buf.readLong();
                long end = buf.readLong();
                
                remainSplit = new FileSplit(new Path(path),pos,end-pos,(String[])null);
                numSplits = n;
                timePerByte = tpb;
                this.remainBytes = remainBytes;
            } else {
            */
                // FIXME raw contains partition information
                DataInputBuffer buffer = new DataInputBuffer();
                buffer.reset(raw, raw.length - 4,4);
                int nParts = buffer.readInt();
                
                buffer.reset(raw, 0, raw.length - 4);
                
                ro = new RelativeOffset();
                ro.readFields(buffer);
                
                LOG.info("loading "+nParts+" partitions. relative offset = "+ro);
                
                partitions = new ArrayList<Partition>(nParts);
                
                for ( int i = 0; i < nParts; ++i ) {
                    MapInputPartition p = new MapInputPartition();
                    p.readFields(buffer);
                    p.setIndex(i);
                    partitions.add(p);
                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug(p);
                    }
                }
                
                long offset = ((MapInputPartition)(partitions.get(0))).getOffset();
                long lastOffset = ((MapInputPartition)(partitions.get(nParts-1))).getLastOffset();
                
                org.apache.hadoop.mapreduce.lib.input.FileSplit newSplit = tip.getNewFileInputSplit();
                remainSplit = new FileSplit(newSplit.getPath(),offset,lastOffset-offset,newSplit.getLocations());
                numSplits = n;
                timePerByte = tpb;
                this.remainBytes = remainBytes;
//            }
            
            if ( LOG.isInfoEnabled() ) {
                LOG.info("constructing remain split "+remainSplit);
            }
        }

        @Override
        public void initScanTask(Job job) throws IOException {
            // scan task requires to set from where to start
            org.apache.hadoop.mapreduce.lib.input.FileSplit newSplit = tip.getNewFileInputSplit();
            String[] hosts = newSplit.getLocations();
            if ( hosts != null && hosts.length > 1 ) {
                // swap the first and the last to prevent overload a single host
                String x = hosts[0];
                hosts[0] = hosts[hosts.length-1];
                hosts[hosts.length-1] = x;
            }
            newSplit = new org.apache.hadoop.mapreduce.lib.input.FileSplit(remainSplit.getPath(),remainSplit.getStart(),remainSplit.getLength(),hosts);
            InputSplitCache.set(job.getConfiguration(),Collections.singletonList(newSplit));
            
            ro.set(job.getConfiguration(),remainSplit.getStart());
            
            if ( LOG.isInfoEnabled() ) {
                LOG.info("input split for scan = "+newSplit);
            }
        }
        
        @Override
        public void initReactiveTask(JobInProgress newJob,Job job) throws IOException {
            Configuration conf = job.getConfiguration();
            ReactiveMapPlan mapPlan = (ReactiveMapPlan)getPlan();
            List<?> splits = mapPlan.getSplits();
            InputSplitCache.set(conf, splits);
            
            ro.set(conf,remainSplit.getStart());
            
            newJob.initializeReactiveMap(splits.size());
        }

        @Override
        public <T> T getContext() {
            return (T)remainSplit; 
        }
    }
    
    public static class TakeOverReduce extends ReactionContext {
        final byte[] key;
        
        public TakeOverReduce(TaskInProgress tip,TaskAttemptID attempt,byte[] key,float tpb,long remainBytes,int code) throws IOException {
            super(tip,attempt);
            /* OLD
            if ( code == 0 ) {
                this.key = key;
            } else {*/
                DataInputBuffer buffer = new DataInputBuffer();
                buffer.reset(key, key.length - 4,4);
                int nParts = buffer.readInt();
                
                buffer.reset(key, 0, key.length - 4);
                int minKeyLen = WritableUtils.readVInt(buffer);
                this.key = new byte[minKeyLen];
                buffer.readFully(this.key);

                LOG.info("minimum key = "+Utils.toHex(this.key));
                LOG.info("loading "+nParts+" partitions");
                
                partitions = new ArrayList<Partition>(nParts);
                for ( int i = 0; i < nParts; ++i ) {
                    MapOutputPartition p = new MapOutputPartition();
                    p.readFields(buffer);
                    p.setIndex(i);
                    partitions.add(p);
                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug(p);
                    }
                }
//            }
            this.timePerByte = tpb;
            this.remainBytes = remainBytes;
        }

        @Override
        public <T> T getContext() {
            return (T)key;
        }

        @Override
        public void initScanTask(Job job) throws IOException {
            if ( key != null && key.length > 0 ) {
                job.getConfiguration().set(SkewTuneJobConfig.REACTIVE_REDUCE_MIN_KEY, Base64.encodeToString(key, false));
            } else {
                LOG.info(getTaskID() + ": setup reactive task as speculative task");
            }
        }

        @Override
        public void initReactiveTask(JobInProgress newJob, Job job) throws IOException {
            initScanTask(job);
            
            // FIXME setup bucket information
            if ( plan != null ) {
                ReactiveReducePlan reducePlan = (ReactiveReducePlan)getPlan();
                Configuration conf = job.getConfiguration();
                boolean newApi = conf.getBoolean("mapred.mapper.new-api",false);
                String partClass = reducePlan.getPartitionClassName( newApi  );
                LOG.debug("setting partitioner class to "+partClass);
                
                if ( newApi ) {
                    conf.set(JobContext.PARTITIONER_CLASS_ATTR, partClass);
                } else {
                    conf.set("mapred.partitioner.class", partClass);
                }
                
                DataOutputBuffer buffer = new DataOutputBuffer(8192);

                Class<?> codecClass = conf.getClass(JobContext.MAP_OUTPUT_COMPRESS_CODEC,DefaultCodec.class);
                CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
                DataOutputStream output = new DataOutputStream(codec.createOutputStream(buffer));
                reducePlan.write(output);
                output.close();
                
                if ( LOG.isDebugEnabled() ) {
                    LOG.debug("compressed partition information = "+buffer.getLength());
                }
                
                String enc = Base64.encodeToString(buffer.getData(),0,buffer.getLength(),false);
//                String enc = Base64.encode(buffer.getData(), buffer.getLength());
                conf.set(SKEWTUNE_REPARTITION_BUCKETS, enc);
                
            	if ( plan.getScheduleOrder() != null ) {
    	        	int[] order = plan.getScheduleOrder();
    	        	buffer.reset();
    	        	
    	            output = new DataOutputStream(codec.createOutputStream(buffer));
    	            for ( int i : order ) {
    	            	WritableUtils.writeVInt(output, i);
    	            }
    	            output.close();
    	            
    	            if ( LOG.isDebugEnabled() ) {
    	                LOG.debug("compressed schedule information = "+buffer.getLength());
    	            }
    	            
    	            enc = Base64.encodeToString(buffer.getData(),0,buffer.getLength(),false);
    	            conf.set(SKEWTUNE_REPARTITION_SCHEDULES, enc);            
            	}
            }
        }
    }
    
    JobInProgress createReactiveJob(TaskID attempt, int n, boolean speculative,ReactionContext reactionContext) throws IOException, InterruptedException {

        // JobInProgress subjob = new JobInProgress(cluster,
        // attempt.getTaskType() == TaskType.MAP ? JobType.MAP_REACTIVE
        // : JobType.REDUCE_REACTIVE, this, attempt.getId(), null,
        // getOriginalJob().getConfiguration(), ugi);
        JobInProgress subjob = new JobInProgress(
                attempt.getTaskType() == TaskType.MAP ? JobType.MAP_REACTIVE
                        : JobType.REDUCE_REACTIVE, this, attempt.getId(), speculative, reactionContext.getPlan());

        // the subjob should have a new token storage since at the end of its
        // execution,
        // the delegation tokens will be expired.
        // FIXME check out the whole authentication business. this job tracker
        // should submit the job on behalf of the user.
        
        // setup security related features
        subjob.setTokenStorage(new TokenStorage());
        
        boolean recursive = getJobType() != JobType.ORIGINAL;
        
//        boolean takeover = subjob.getConfiguration().getBoolean(ENABLE_SKEWTUNE_TAKEOVER, false);
        if (subjob.getJobType() == JobType.MAP_REACTIVE) {
            // setup configurations for map
            setupReactiveMap(this, attempt, subjob, n);
//            takeover = subjob.getConfiguration().getBoolean(ENABLE_SKEWTUNE_TAKEOVER_MAP, takeover);
        } else {
            if ( recursive ) {
                setupRecursiveReactiveReduce(this, attempt, subjob, n);
            } else {
                setupReactiveReduce(this, attempt, subjob, n);
            }
//            takeover = subjob.getConfiguration().getBoolean(ENABLE_SKEWTUNE_TAKEOVER_REDUCE, takeover);
        }
        
        if ( reactionContext != null ) {
//            postHook.run(subjob);
            reactionContext.initReactiveTask(subjob, subjob.getJob());
        }
        
        Configuration conf = subjob.getConfiguration();
//        if ( takeover && speculative ) {
        if ( speculative ) {
            conf.setBoolean(ENABLE_SKEWTUNE_TAKEOVER,false);
            conf.setBoolean(ENABLE_SKEWTUNE_TAKEOVER_MAP,false);
            conf.setBoolean(ENABLE_SKEWTUNE_TAKEOVER_REDUCE,false);
        } else {
            // setup job dependency
            conf.set("mapreduce.job.parent",attempt.getJobID().toString());
        }
//            Plan plan = postHook.getPlan();
//            if ( plan != null && plan instanceof ReactiveReducePlan ) {
//                ReactiveReducePlan reducePlan = (ReactiveReducePlan)plan;
//                LOG.debug("setting partitioner class to "+reducePlan.getPartitionClassName());
//                
//                if ( conf.getBoolean("mapred.mapper.new-api",false) ) {
//                    conf.set(JobContext.PARTITIONER_CLASS_ATTR, reducePlan.getPartitionClassName());
//                } else {
//                    conf.set("mapred.partitioner.class", reducePlan.getPartitionClassName());
//                }
////              subjob.getJob().setNumReduceTasks(plan.getNumPartitions());
//                reducePlan.setBucketInfo(conf);
//            }
//        }
        
        return subjob;
    }

    // ----

    private static void setupReactiveJob(JobInProgress oldOne, TaskID taskid,
            JobInProgress newOne) {
        // setup common configuration
        Job newJob = newOne.getJob();
        newJob.getConfiguration().setBoolean(SKEWTUNE_REACTIVE_JOB, true);
        newJob.getConfiguration().set(ORIGINAL_JOB_ID_ATTR, oldOne.getJobID().toString());
        newJob.getConfiguration().set(ORIGINAL_TASK_ID_ATTR, taskid.toString());
        newJob.setJobName(getNewJobName(oldOne.getJob().getJobName(), taskid));

        // turn off speculative execution
        if ( taskid.getTaskType() == TaskType.MAP ) {
            newJob.setMapSpeculativeExecution(false);
            newJob.setReduceSpeculativeExecution(false);
        } else {
            // we use speculative execution for reactive reduce
            newJob.setMapSpeculativeExecution(false);
            newJob.setReduceSpeculativeExecution(false);
        }

        // turn off map input monitoring
        newJob.getConfiguration().set(MAP_INPUT_MONITOR_URL_ATTR, "");

        // remove jar file and add it to the cache
        // everyfiles that we need has been copied to job tracker. by pass all
        // copy business
        // newJob.setJar("");
    }

    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
    static {
    	NUMBER_FORMAT.setMinimumIntegerDigits(5);
    	NUMBER_FORMAT.setGroupingUsed(false);
    }

    private synchronized static String getOutputPathName(TaskID taskid) {
    	StringBuilder buf = new StringBuilder();
    	buf.append( taskid.getTaskType() == TaskType.MAP ? "m" : "r")
    		.append('-')
    		.append(NUMBER_FORMAT.format(taskid.getId()));
    	return buf.toString();
    }
    
    private static String getRecursiveOutputPathName(String outputName, int partition,int numTasks) {
    	// simply append the current partition id to the previous output name
    	NumberFormat fmt = NumberFormat.getInstance();
    	fmt.setMinimumIntegerDigits(Integer.toString(numTasks).length());
    	fmt.setGroupingUsed(false);
    	return outputName + "." + fmt.format(partition);
    }
    

    private static void setupReactiveMap(JobInProgress oldOne, TaskID taskid,
            JobInProgress newOne, int nSplits) throws IOException {
        setupReactiveJob(oldOne, taskid, newOne);

        final JobInProgress orgJob = oldOne.getOriginalJob();

        Job newJob = newOne.getJob();
        Configuration conf = newJob.getConfiguration();
        boolean useNewApi = conf.getBoolean("mapred.mapper.new-api", false);
        
        // load input split
        TaskInProgress tip = oldOne.getTaskInProgress(taskid);

        // FIXME determine the number of mappers
        int numSplits = nSplits == 0 ? 2 : nSplits;

        Path orgOutDir = oldOne.getOriginalJob().getOutputPath();
        Path outDir = new Path(orgOutDir, getOutputPathName(taskid));
        FileOutputFormat.setOutputPath(newJob, outDir);

        FileSystem fs = outDir.getFileSystem(conf);
        if (fs.exists(outDir)) {
            fs.delete(outDir, true);
        }

        if (useNewApi) {
            conf.set(ORIGINAL_INPUT_FORMAT_CLASS_ATTR, conf.get(INPUT_FORMAT_CLASS_ATTR));
            String partitionerClass = conf.get(PARTITIONER_CLASS_ATTR);
            if (partitionerClass != null) {
                conf.set(ORIGINAL_PARTITIONER_CLASS_ATTR, partitionerClass);
            }

            newJob.setInputFormatClass(DelegateInputFormat.class);
            
            /*
            org.apache.hadoop.mapreduce.InputSplit split = tip.getInputSplit();
            // FIXME should support customizable input split partitioner
            List<org.apache.hadoop.mapreduce.InputSplit> subSplits = InputSplitPartitioner.split(conf, split, numSplits);
            InputSplitCache.set(conf, subSplits);

            if (LOG.isDebugEnabled()) {
                LOG.debug(subSplits);
            }
            */

            if (orgJob.getNumReduceTasks() > 0) {
                newJob.setOutputFormatClass(skewtune.mapreduce.lib.output.DummyFileOutputFormat.class);
            } // otherwise, we reuse the output format class. no need to sort.
        } else {
            // FIXME everything is hardcoded here... :-(
            conf.set(ORIGINAL_INPUT_FORMAT_CLASS_ATTR, conf.get("mapred.input.format.class"));
            String partitionerClass = conf.get("mapred.partitioner.class");
            if (partitionerClass != null) {
                conf.set(ORIGINAL_PARTITIONER_CLASS_ATTR, partitionerClass);
            }

            conf.setBoolean("mapred.mapper.new-api", false); // force
            conf.setBoolean("mapred.reducer.new-api", false);

            if (orgJob.getNumReduceTasks() > 0) {
                conf.setClass("mapred.output.format.class",
                        skewtune.mapred.DummyFileOutputFormat.class,
                        org.apache.hadoop.mapred.OutputFormat.class);
            } // otherwise, reuse the original output format.
            conf.setClass("mapred.input.format.class",
                    skewtune.mapred.DelegateInputFormat.class,
                    org.apache.hadoop.mapred.InputFormat.class);
        }
        
        // setup original number of reduces so that the map output partitioned
        // in a compatible manner.
        conf.setInt(ORIGINAL_NUM_REDUCES, newJob.getNumReduceTasks());
        conf.setInt(MAP_OUTPUT_REPLICATION_ATTR, 5); // some random number

        // force sort the output
        if (newJob.getNumReduceTasks() > 0) {
            newJob.getConfiguration().setBoolean(MAP_OUTPUT_SORT_ATTR, true);
        }

        newJob.setNumReduceTasks(0); // will be always map only job
    }

    @SuppressWarnings("unchecked")
    private static void setupReactiveReduce(JobInProgress oldOne,
            TaskID taskid, JobInProgress newOne, int nSplits)
            throws IOException {
        setupReactiveJob(oldOne, taskid, newOne);

        // setup configurations for reduce
        // reduce needs to retrieve output from original job.

        // wait until token is loaded
        TokenStorage oldTokenStorage = oldOne.getTokenStorage(true);
        Token<JobTokenIdentifier> jt = (Token<JobTokenIdentifier>) TokenCache.getJobToken(oldTokenStorage);
        if (LOG.isDebugEnabled()) {
            LOG.debug(oldOne.getJobID().toString() + " job token password = "
                    + Arrays.toString(jt.getPassword()));
        }

        newOne.getTokenStorage().addToken(new Text(oldOne.getJobID().toString()), jt);

        if (LOG.isDebugEnabled()) {
            LOG.debug(oldOne.getJobID() + " password = "
                    + Arrays.toString(jt.getPassword()));
            LOG.debug(oldOne.getJobID()
                    + " secret = "
                    + Arrays.toString(JobTokenSecretManager.createSecretKey(
                            jt.getPassword()).getEncoded()));
        }

        final Job newJob = newOne.getJob();
        final Configuration conf = newJob.getConfiguration();

        newJob.setTokenStorage(newOne.getTokenStorage());

        // we need a new input format

        // we need to check the API because of the combiner and partitioner
        // interface

        boolean useNewApi = conf.getBoolean("mapred.reducer.new-api", false);
        boolean recursive = oldOne.getJobType() == JobType.REDUCE_REACTIVE;

        // do we have a special partitioner?
        String partClassName = newJob.getConfiguration().get(REACTIVE_PARTITIONER_CLASS_ATTR);

        final int numSplits = nSplits == 0 ? 2 : nSplits;

        if (useNewApi) {
            newJob.setInputFormatClass(MapOutputInputFormat.class);
            newJob.setMapperClass(Mapper.class);
            List<MapOutputSplit> splits = oldOne.getMapOutputSplits(taskid).splits;
            InputSplitCache.set(conf, splits);

            // partitioner
            if (partClassName == null) {
                // no special partitioner has assigned.
                // two choices. with original partitioner, bump up the number of
                // partitions.
                // the other choice is that using range partition.
                String orgCls = conf.get(PARTITIONER_CLASS_ATTR);
                if (orgCls == null) {
                    conf.setClass(
                            ORIGINAL_PARTITIONER_CLASS_ATTR,
                            org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class,
                            org.apache.hadoop.mapreduce.Partitioner.class);
                } else {
                    conf.set(ORIGINAL_PARTITIONER_CLASS_ATTR, orgCls);
                }
                int newPrime = PrimeTable.getNearestPrime(Math.max(oldOne
                        .getNumReduceTasks(), numSplits));
                if (LOG.isInfoEnabled()) {
                    LOG.info("setting up a new prime number = " + newPrime);
                }
                conf.setInt(DELEGATE_PARTITIONER_NUM_VIRTUAL_REDUCES, newPrime);
                newJob.setPartitionerClass(skewtune.mapreduce.lib.partition.DelegatePartitioner.class);
            } else {
                // easy case! switch to the special partition
                newJob.getConfiguration().set(PARTITIONER_CLASS_ATTR,partClassName);
            }
        } else {
            conf.setClass("mapred.input.format.class",
                    MapOutputInputFormat.class,
                    org.apache.hadoop.mapred.InputFormat.class);
            conf.setClass("mapred.mapper.class",
                    org.apache.hadoop.mapred.lib.IdentityMapper.class,
                    org.apache.hadoop.mapred.Mapper.class);
            List<MapOutputSplit> splits = oldOne.getMapOutputSplits(taskid).splits;
            InputSplitCache.set(conf, splits);

            if (partClassName == null) {
                String orgCls = conf.get("mapred.partitioner.class");
                if (orgCls == null) {
                    conf.setClass(ORIGINAL_PARTITIONER_CLASS_ATTR,
                            org.apache.hadoop.mapred.lib.HashPartitioner.class,
                            org.apache.hadoop.mapred.Partitioner.class);
                } else {
                    conf.set(ORIGINAL_PARTITIONER_CLASS_ATTR, orgCls);
                }
                int newPrime = PrimeTable.getNearestPrime(Math.max(oldOne
                        .getNumReduceTasks(), numSplits));
                if (LOG.isInfoEnabled()) {
                    LOG.info("setting up a new prime number = " + newPrime);
                }
                conf.setInt(DELEGATE_PARTITIONER_NUM_VIRTUAL_REDUCES, newPrime);
                conf.setClass("mapred.partitioner.class",
                        skewtune.mapred.DelegatePartitioner.class,
                        org.apache.hadoop.mapred.Partitioner.class);
            } else {
                conf.set("mapred.partitioner.class", partClassName);
            }

            conf.setBoolean("mapred.mapper.new-api", false); // force
            conf.setBoolean("mapred.reducer.new-api", false);
        }

        // setup output directory. output format is identical.
        Path orgOutDir = oldOne.getOriginalJob().getOutputPath();
        Path outDir = new Path(orgOutDir, getOutputPathName(taskid));
        FileOutputFormat.setOutputPath(newJob, outDir);

        FileSystem fs = outDir.getFileSystem(conf);
        if (fs.exists(outDir)) {
            fs.delete(outDir, true);
        }

        // FIXME depending on new API or old API, change the mapper class

        if (conf.get(COMBINE_CLASS_ATTR) != null) {
            // FIXME bump up spill size. we know that the mapper does not
            // require much memory. what if we need more complex partition
            // function?
            // for example, what if we have to see all the data to evenly
            // partition the input?
            // conf.setFloat(MAP_SORT_SPILL_PERCENT,0.0f); // 0.8 by default
        }

        // FIXME
        conf.setInt(IO_SORT_MB, 256); // just assign enough memory since we
                                      // know that the map does nothing

        conf.setInt(ORIGINAL_NUM_REDUCES, newJob.getNumReduceTasks());

        // FIXME determine the number of reducers
        newJob.setNumReduceTasks(numSplits);
    }

    public void setSplits(TaskSplitMetaInfo[] metaSplits) {
        if (metaSplits.length != numMaps) {
            // something is wrong
            LOG.error("the number of maps from configuration and splits does not match: "
                    + getJobID().toString());
        }

        for (int i = 0; i < numMaps; ++i) {
            tasks[i].setMetaSplit(metaSplits[i]);
        }
    }

    public synchronized boolean hasReactiveJob(TaskID taskid) {
        int index = taskid.getTaskType() == TaskType.MAP ? taskid.getId()
                : taskid.getId() + numMaps;
        return completedReactiveJobs.get(index)
                || getTaskInProgress(taskid).getReactiveJob() != null;
    }
    
    public boolean isReactiveJob() {
        return parent != null;
    }

    public synchronized void registerReactiveJob(TaskID taskid,
            JobInProgress subJob) {
        reactiveJobs.put(subJob.getDowngradeJobID(), subJob);
        getTaskInProgress(taskid).setReactiveJob(subJob);
        if (taskid.getTaskType() == TaskType.MAP) {
            reactiveMaps.put(taskid.getId(), subJob);
//            if ( ! subJob.isSpeculative() ) {
//                this.newTakeOver.add(new TaskStatusEvent(taskid,TaskState.TAKEOVER));
////                this.newTakeOver.add(taskid.getId());
//            }
            LOG.info("registering reactive job for "+taskid+" . speculation="+subJob.isSpeculative());
        } else {
            reactiveReduces.put(taskid.getId(), subJob);
        }
    }

    public synchronized void unregisterReactiveJob(JobInProgress subJob) {
        reactiveJobs.remove(subJob.getJobID());
        getTaskInProgress(subJob.getParentTaskID()).setReactiveJob(null);
        TaskID taskid = subJob.getParentTaskID();
        if (taskid.getTaskType() == TaskType.MAP) {
            reactiveMaps.remove(taskid.getId());
        } else {
            reactiveReduces.remove(taskid.getId());
        }
    }

    // below assumes sycnhronized outside of methods

    Future<Boolean> eventSplitMeta;

    volatile Future<Boolean> eventJobToken;
    volatile Future<Boolean> eventCompletionEvents;

    void waitUntilReadyToSplit(TaskID taskid) throws InterruptedException,
            ExecutionException {
        eventSplitMeta.get();
        if (eventJobToken == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("wait for loading job token for " + job.getJobID());
            }
            synchronized (this) {
                while (eventJobToken == null) {
                    wait();
                }
            }
        }
        eventJobToken.get();

        if (taskid.getTaskType() == TaskType.REDUCE) {
            canSplitReduce(true);
        }
    }

    /**
     * unregister this reactive job from parents and reclaim the space
     */
    public void cleanup() {
        if (type != JobType.ORIGINAL)
            parent.unregisterReactiveJob(this);
    }

    // update task completion event
    List<TaskCompletionEvent> completedMapEvents;

    int lastCompletionEvent;

    static final TaskCompletionEvent[] EMPTY_EVENTS = new TaskCompletionEvent[0];

    // must acquire synchronization on this
    // and also the completedMapEvents.
    //
    // private synchronized boolean isAllMapOutputAvailable() {
    // BitSet tmp = this.completedTasks.get(0, numMaps);
    // tmp.or(completedReactiveJobs.get(0, numMaps));
    // return tmp.cardinality() == numMaps;
    // }
    
    /**
     * only called for recursive reactive reduce jobs
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean updateMapCompletionEvents() throws IOException, InterruptedException {
        BitSet completedMaps = new BitSet(numMaps);
        TaskCompletionEvent[] events = job.getTaskCompletionEvents(lastCompletionEvent, numMaps << 1);
    
        if (LOG.isDebugEnabled() && events.length > 0) {
            LOG.debug(String
                    .format("task completion events for %s from event id = %d, up to %d, # of retrieved completion events = %d",
                            getJobID(), lastCompletionEvent, numMaps,
                            events.length));
        }

        synchronized (completedMapEvents) {
            lastCompletionEvent += events.length;
            for (TaskCompletionEvent event : events) {
                if (event.getStatus() == TaskCompletionEvent.Status.SUCCEEDED) {
                    int taskid = event.getTaskAttemptId().getTaskID().getId();
                    if (event.isMapTask()) {
                        // add this to job in progress for future reduce split
                        completedMapEvents.add(event);
                        completedMaps.set(taskid);
                    }
                }
            }
        }

        return completedMaps.cardinality() >= numMaps;
    }

    public boolean updateTaskCompletionEvent() throws IOException,
            InterruptedException {
        if (job.isComplete()) {
            return false;
        }

        // if ( LOG.isDebugEnabled() ) {
        // LOG.debug(String.format("Retrieving task completion events for %s from event id = %d, up to %d",getJobID(),lastCompletionEvent,numMaps));
        // }

        TaskCompletionEvent[] events = job.getTaskCompletionEvents(lastCompletionEvent, numMaps << 1);

        if (LOG.isTraceEnabled() && events.length > 0) {
            LOG.trace(String
                    .format("task completion events for %s from event id = %d, up to %d, # of retrieved completion events = %d",
                            getJobID(), lastCompletionEvent, numMaps,
                            events.length));
        }

        BitSet completedMaps = new BitSet(numMaps);
        synchronized (completedMapEvents) {
            lastCompletionEvent += events.length;
            for (TaskCompletionEvent event : events) {
                if (event.getStatus() == TaskCompletionEvent.Status.SUCCEEDED) {
                    int taskid = event.getTaskAttemptId().getTaskID().getId();
                    if (event.isMapTask()) {
                        // add this to job in progress for future reduce split
                        completedMapEvents.add(event);
                        completedMaps.set(taskid);
//                        LOG.debug("completed map = "+event.getTaskAttemptId());
                    } else if ( event.getTaskAttemptId().getTaskType() == TaskType.REDUCE ) {
//                        completedTasks.set(event.getTaskAttemptId().getTaskID().getId()+numMaps);
                        taskid += numMaps;
                        JobInProgress subJip = tasks[taskid].getReactiveJob();
                        if ( subJip != null && subJip.isSpeculative() ) {
                            // only speculative task needs to be killed!
                            tasks[taskid].getReactiveJob().kill();
                        }
                    }
                }
            }
            //
            // if ( completedMapEvents.size() >= numMaps ) {
            // completedMapEvents.notifyAll();
            // }
        }
        
//        if ( LOG.isDebugEnabled() && ! completedMaps.isEmpty() ) {
//            LOG.debug("completed maps = "+completedMaps);
//        }

        if ( availMapOutput != null && availMapOutput.updateOriginalTask(completedMaps) ) {
//            LOG.info("all map output became available for "+job.getJobID());
        }

        return true;
    }
    
    /**
     * retrieve completed map outputs executed in the given tracker node
     * @param tracker
     * @return
     */
    public List<TaskID> getLostMapOutput(String tracker) {
        ArrayList<TaskID> result = new ArrayList<TaskID>();
        synchronized (completedMapEvents) {
            for ( TaskCompletionEvent event : completedMapEvents ) {
                if ( event.getTaskTrackerHttp().startsWith(tracker) ) {
                    result.add(TaskID.downgrade(event.getTaskAttemptId().getTaskID()));
                }
            }
        }
        return result;
    }

    private String extractHost(String httpBaseUrl) {
        // TODO currently the URL format is fixed to http://hostname:port. may
        // need to change in the future if security is added.
        int colon = httpBaseUrl.lastIndexOf(':');
        return httpBaseUrl.substring(7, colon);
    }

    public boolean canSplitReduce(boolean wait) throws InterruptedException {
        if (wait) {
            // FIXME if reactive map output is available the reduce is
            // splittable!
            // synchronized ( completedMapEvents ) {
            // while ( completedMapEvents.size() < numMaps ) {
            // completedMapEvents.wait(1000);
            // }
            // }
            // synchronized (completedMapEvents) { // this gets hairy
            // while ( completedMapEvents.size() < numMaps && !
            // isAllMapOutputAvailable() ) {
            // completedMapEvents.wait(1000);
            // }
            // }

            availMapOutput.allAvailable(true);

            if (LOG.isDebugEnabled()) {
                LOG.debug("ready to split a reduce!");
            }
            return true;
        } else {
            return availMapOutput.allAvailable(false);
            // synchronized ( completedMapEvents ) {
            // return completedMapEvents.size() >= numMaps ||
            // isAllMapOutputAvailable();
            // }
        }
    }

    /**
     * 
     * @return
     * @throws IOException
     */
    class MapOutputSplitInfo {
        /**
         * 
         */
        public final int numMapOutput;
        public final List<MapOutputSplit> splits;
        
        MapOutputSplitInfo(int n,List<MapOutputSplit> s) {
            this.numMapOutput = n;
            this.splits = s;
        }
    }
    
    public MapOutputSplitInfo getMapOutputSplits(TaskID taskid)
            throws IOException {
        TaskCompletionEvent[] events = null;

        // now go over the missing task id
        List<MapOutputAndHost> reOutput = new ArrayList<MapOutputAndHost>(numMaps);

        synchronized (completedMapEvents) {
            events = completedMapEvents.toArray(EMPTY_EVENTS);
            if (completedMapEvents.size() < numMaps) {
                LOG.warn(String.format(
                        "only %d out of %d map outputs are available for %s!",
                        events.length, numMaps, getJobID()));
            }
        }

        // onlyInReactive() should take care of take over reactive map outputs as well
        BitSet reOuts = availMapOutput.onlyInReactive();
        if (!reOuts.isEmpty()) {
            synchronized (this) {
                for (int i = 0; i < numMapOutput; ++i) {
                    if (reOuts.get(newMapOutput[i].getPartition())) {
                        reOutput.add(newMapOutput[i]);
                    }
                }
            }
        }
        

        HashMap<String, MapOutputSplit> splits = new HashMap<String, MapOutputSplit>();
        ArrayList<MapOutputSplit> ret = new ArrayList<MapOutputSplit>(splits.size() * 2);

        for (TaskCompletionEvent event : events) {
            String uri = event.getTaskTrackerHttp();
            String host = extractHost(uri);
            MapOutputSplit split = splits.get(host);
            if (split == null) {
                split = new MapOutputSplit(host, uri);
                splits.put(host, split);
            }
            split.add(event.getTaskAttemptId());
        }
        
        ret.addAll(splits.values());
        
        int nfiles = ret.size();

        // now incorporate reactive outputs
        int spilled = 0;
        int reduceId = taskid.getId();
        final long CHUNKSIZE = 128*1024*1024;
        HashMap<String, MapOutputSplit> reactiveSplits = new HashMap<String, MapOutputSplit>();
        for (MapOutputAndHost reout : reOutput) {
            // the index contains a series of offset length pair for the reduce.
            int mapId = reout.getPartition();
            ReactiveMapOutput output = reout.getMapOutput();
            MapOutputIndex index = output.getOutputIndex(reduceId);
            String[] hosts = reout.getHosts();
            
            nfiles += output.getNumIndexes();

            // now we can construct a file split and group them per host
            // each split must include following information.
            // (map id, num split, offset, raw length, compressed length)
            // now it's outputpath/m-<partition>-<# splits>

            for (int i = 0; i < hosts.length; ++i) {
                MapOutputSplit split = reactiveSplits.get(hosts[i]);
                MapOutputIndex.Record entry = index.getIndex(i);
                if ( entry.getRawLength() > CHUNKSIZE ) {
                    // phew! this should be chopped off!!
                    long off = 0;
                    long remain = entry.getRawLength();
                    while ( remain > 0 ) {
                        long sz = Math.min(remain,CHUNKSIZE);
                        MapOutputSplit nsplit = new MapOutputSplit( sz < CHUNKSIZE ? hosts[i] : null );
                        nsplit.add(mapId, output.getNumIndexes(), i, entry, off, sz);
                        ret.add(nsplit);
                        remain -= sz;
                        off += sz;
                        ++spilled;
                    }
                    if ( split != null ) {
                        // unset the host of the current since we already got something to do tons of work
                        split.setHost(null);
                    }
                } else {
                    if (split == null) {
                        split = new MapOutputSplit(hosts[i]);
                        reactiveSplits.put(hosts[i], split);
                    }
                    if ( entry.getRawLength() + split.getRawLength() > CHUNKSIZE ) {
                        ret.add(split);
                        split = new MapOutputSplit();
                        reactiveSplits.put(hosts[i], split);
                        ++spilled;
                    }
                    split.add(mapId, output.getNumIndexes(), i, entry);
                    // should balance the amount of data read by this split
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Total " + splits.size() + " tasktracker splits, "
                    + (reactiveSplits.size()+spilled) + " reactive splits for " + taskid);
        }
//        ret.addAll(splits.values());
        ret.addAll(reactiveSplits.values());
        return new MapOutputSplitInfo(nfiles,ret);
    }
    
    private synchronized void findSpeculativeTaskInReactiveJob(int availMaps,int availReduces,long now,List<STTaskStatus> candidates) throws InterruptedException {
        boolean speculateMap = availMaps > 0 && canSpeculateMapNew();
        boolean speculateReduce = availReduces > 0 && canSpeculateReduceNew();
        
        /*
        if ( getJobType() == JobType.MAP_REACTIVE && speculateMap ) {
            for (int i = 0; i < numMaps; ++i) {
                if (!completedReactiveJobs.get(i)) {
                    tasks[i].getAllTaskStatus(candidates, now);
                } // we already have some output. don't bother to reschedule!
            }
        }
        */

        if ( getJobType() == JobType.REDUCE_REACTIVE
                && speculateReduce
                && ((availMapOutput == null && numCompletedMaps == numMaps) || (availMapOutput != null && availMapOutput.allAvailable(false))) ) {
            for (int i = numReduces; i < tasks.length; ++i) {
                if (!completedReactiveJobs.get(i)) {
                    tasks[i].getAllTaskStatus(candidates, now);
                } // we already have some output. don't bother to reschedule!
            }
        }
        
        if ( ! reactiveJobs.isEmpty() ) {
            for ( JobInProgress jip : this.reactiveJobs.values() ) {
                jip.findSpeculativeTaskInReactiveJob(availMaps, availReduces, now, candidates);
            }
        }
    }
    

    public synchronized List<STTaskStatus> findSpeculativeTaskNew(int availMaps,
            int availReduces) throws InterruptedException {
        boolean speculateMap = availMaps > 0 && canSpeculateMapNew();
        boolean speculateReduce = availReduces > 0 && canSpeculateReduceNew();

        // if a speculative task is running, it must be all scheduled.
        ArrayList<STTaskStatus> candidates = new ArrayList<STTaskStatus>(tasks.length * 2);
        final long now = System.currentTimeMillis();

        if (speculateMap) {
            for (int i = 0; i < numMaps; ++i) {
                if (!completedReactiveJobs.get(i)) {
                    tasks[i].getAllTaskStatus(candidates, now);
                } // we already have some output. don't bother to reschedule!
            }
        }

        if (speculateReduce && availMapOutput.allAvailable(false) ) {
            for (int i = numReduces; i < tasks.length; ++i) {
                if (!completedReactiveJobs.get(i)) {
                    tasks[i].getAllTaskStatus(candidates, now);
                } // we already have some output. don't bother to reschedule!
            }
        }
        
        if ( ! reactiveJobs.isEmpty() ) {
            for ( JobInProgress jip : reactiveJobs.values() ) {
                jip.findSpeculativeTaskInReactiveJob(availMaps,availReduces,now,candidates);
            }
        }

        // now we got candidates
        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        // FIXME currently, take just one for map and another for reduce
        ArrayList<STTaskStatus> list = new ArrayList<STTaskStatus>(2);
        boolean foundMap = availMaps == 0; // otherwise, we do not have space
                                           // for a map
        boolean foundReduce = availReduces == 0; // otherwise we do not have
                                                 // space for a reduce

        Collections.sort(candidates, new EstimatedTimeComparator(now));

        for (STTaskStatus candidate : candidates) {
            if (candidate.getTaskID().getTaskType() == TaskType.MAP) {
                if (!foundMap) {
                    list.add(candidate);
                    foundMap = true;
                }
            } else {
                if (!foundReduce) {
                    list.add(candidate);
                    foundReduce = true;
                }
            }

            if (foundMap && foundReduce)
                break;
        }

        if (LOG.isInfoEnabled()) {
            for (STTaskStatus target : list) {
                LOG.info("speculative execution candidate: "
                        + target.getTaskID().getTaskID() + " (expected "
                        + (target.getRemainTime(now)) + " secs)");
            }
        }

        // finally double check whether this is speculatable
        return list;
    }



    public synchronized List<STTaskStatus> findSpeculativeTask(int availMaps,
            int availReduces) throws InterruptedException {
        boolean speculateMap = availMaps > 0 && canSpeculateMapNew();
        boolean speculateReduce = availReduces > 0 && canSpeculateReduceNew();

        // if a speculative task is running, it must be all scheduled.

        if (!speculateMap && !speculateReduce) {
            return Collections.emptyList();
        }

        ArrayList<STTaskStatus> candidates = new ArrayList<STTaskStatus>(tasks.length * 2);
        final long now = System.currentTimeMillis();

        if (speculateMap) {
            for (int i = 0; i < numMaps; ++i) {
                if (!completedReactiveJobs.get(i)) {
                    tasks[i].getAllTaskStatus(candidates, now);
                } // we already have some output. don't bother to reschedule!
            }
        }

        if (speculateReduce && availMapOutput.allAvailable(false) ) {
            for (int i = numReduces; i < tasks.length; ++i) {
                if (!completedReactiveJobs.get(i)) {
                    tasks[i].getAllTaskStatus(candidates, now);
                } // we already have some output. don't bother to reschedule!
            }
        }

        // now we got candidates
        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        // FIXME currently, take just one for map and another for reduce
        ArrayList<STTaskStatus> list = new ArrayList<STTaskStatus>(2);
        boolean foundMap = availMaps == 0; // otherwise, we do not have space
                                           // for a map
        boolean foundReduce = availReduces == 0; // otherwise we do not have
                                                 // space for a reduce

        Collections.sort(candidates, new EstimatedTimeComparator(now));

        for (STTaskStatus candidate : candidates) {
            if (candidate.getTaskID().getTaskType() == TaskType.MAP) {
                if (!foundMap) {
                    list.add(candidate);
                    foundMap = true;
                }
            } else {
                if (!foundReduce) {
                    list.add(candidate);
                    foundReduce = true;
                }
            }

            if (foundMap && foundReduce)
                break;
        }

        if (LOG.isInfoEnabled()) {
            for (STTaskStatus target : list) {
                LOG.info("speculative execution candidate: "
                        + target.getTaskID().getTaskID() + " (expected "
                        + (target.getRemainTime(now)) + " secs)");
            }
        }

        // finally double check whether this is speculatable
        return list;
    }

    static class EstimatedTimeComparator implements Comparator<STTaskStatus> {
        final long now;

        EstimatedTimeComparator(long now) {
            this.now = now;
        }

        @Override
        public int compare(STTaskStatus o1, STTaskStatus o2) {
            double t1 = o1.getRemainTime(now);
            double t2 = o2.getRemainTime(now);

            if (t1 > t2)
                return -1;
            else if (t1 < t2)
                return 1;
            return 0;
        }

    }

    public synchronized void addPathToDelete(Path p) {
        pathToDelete.add(p);
    }

    public synchronized void removePathToDelete(Path p) {
        pathToDelete.remove(p);
    }

    public synchronized void deleteAll(FileSystem fs) {
        for (Path p : pathToDelete) {
            try {
                fs.delete(p, true);
            } catch (IOException e) {
                LOG.warn("failed to delete: " + p);
            }
        }
    }

    /**
     * 
     * @param status
     *            heartbeat message of task
     * @param completed 
     * @return true if task has canceled
     */
    public int handleTaskHeartbeat(STTaskStatus status, String host, BitSet completed) {
        TaskInProgress tip = getTaskInProgress(status.getTaskID());
        boolean firstReport = tip.neverScheduled();
        boolean isMap = tip.getTaskType() == TaskType.MAP;
        
        TaskState oldState = tip.getState();
        if ( tip.setStatus(status, host) ) {
            // committed
            TaskState newState = tip.getState();
            int partition = tip.getPartition() + (isMap ? 0 : numMaps); 
            if ( type == JobType.ORIGINAL ) {
                if ( isMap ) {
                    if ( newState == TaskState.COMPLETE || tip.hasReactiveJobCompleted() ) {
                        completedTasks.set(partition);
                        completed.set(partition);
                    }
                    
                    if ( oldState != newState && newState == TaskState.COMPLETE ) {
                        this.newTakeOver.add(new TaskStatusEvent(status.getTaskID(),TaskState.COMPLETE));
                        if ( LOG.isDebugEnabled() ) {
                            LOG.debug("original map task complete: "+tip.getTaskID());
                        }
                    }
//                    availMapOutput.updateOriginalTask(tip.getPartition());                    
                } else if ( tip.getTaskType() == TaskType.REDUCE ) {
                    if ( newState == TaskState.COMPLETE || tip.hasReactiveJobCompleted() ) {
                        completedTasks.set(partition);
                    } // otherwise, it took over. should not completed yet?
                }
            } else if ( type == JobType.MAP_REACTIVE ) {
                // FIXME if we allow recursive reactive map, the availmapoutput must follow the recursion
                if ( tip.getMapOutputIndex() != null ) {
                    availMapOutIndex.set(partition);
                }
            } else if ( type == JobType.REDUCE_REACTIVE ) {
                if ( isMap ) {
                    // reactive reduce does not have a map task
                    if ( newState == TaskState.COMPLETE ) {
                        if ( ! completedTasks.get(partition) ) ++numCompletedMaps;
                        completedTasks.set(partition);
                        completed.set(partition);
                    }
                } else if ( tip.getTaskType() == TaskType.REDUCE ) {
                    if ( newState == TaskState.COMPLETE || tip.hasReactiveJobCompleted() ) {
                        completedTasks.set(partition);
                    } // otherwise, it took over. should not completed yet?
                }
            }
            
            float v = status.getTimePerByte();
            if ( oldState != newState && v > 0. ) {
                if ( isMap ) {
                    this.mapAvgTimePerByte.add(status.getTimePerByte());
                } else {
                    this.reduceAvgTimePerByte.add(status.getTimePerByte());
                }
            }
        }
        
        if (firstReport) {
            if ( isMap ) {
                ++numScheduledMaps;
            } else {
                ++numScheduledReduces;
            }
        }

        // FIXME do we need to fire notify parent?
        
//        return tip.hasCancelled();
        return tip.getAction(status.getTaskID());
    }

    // we can speculate another if there is enough

    public synchronized boolean canSpeculateMap() {
        if (numScheduledMaps < numMaps)
            return false;
        
        if (reactiveMaps.size() > 0) {
            for (JobInProgress subJob : reactiveMaps.values()) {
                if (!subJob.canSpeculateMap()) {
                    return false;
                }
            }
        }
        return true;
    }
    
    public synchronized boolean canSpeculateMapNew() {
        return numScheduledMaps >= numMaps;
    }
    
    public synchronized boolean canSpeculateReduceNew() throws InterruptedException {
        return numReduces > 0
                && numScheduledReduces >= numReduces
                && (availMapOutput == null || availMapOutput.allAvailable(false));
    }
    
    public synchronized boolean canSpeculateReduce() throws InterruptedException {
        return canSpeculateReduce(true);
    }

    public synchronized boolean canSpeculateReduce(boolean root)
            throws InterruptedException {
        if (numReduces == 0
                || numScheduledReduces < numReduces
                || ( root && !availMapOutput.allAvailable(false)) ) {
            return false;
        }

        if (reactiveReduces.size() > 0) {
            for (JobInProgress subJob : reactiveReduces.values()) {
                if (!subJob.canSpeculateReduce(false)) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean canSpeculateThis(TaskID taskid) throws InterruptedException {
        return (taskid.getTaskType() == TaskType.MAP) ? canSpeculateMap()
                : canSpeculateReduce();
    }

    public void notifyMapCompletion(BitSet completed) {
        availMapOutput.updateOriginalTask(completed);
    }
    
    public boolean isRequired(TaskID taskid) {
        if ( taskid.getTaskType() == TaskType.REDUCE ) return true;
        return availMapOutput.require(taskid.getId());
    }

    public synchronized void addTaskStateEvent(TaskInProgress tip, TaskState state) {
        if ( tip.getTaskType() == TaskType.MAP ) {
            TaskState oldState;
            if ( state == TaskState.CANCEL_TAKEOVER ) {
                if ( tip.getState() == TaskState.WAIT_TAKEOVER ) {
                    // propagate pending COMPLETE as well.
//                    this.newTakeOver.add(new TaskStatusEvent(tip.getTaskID(),TaskState.COMPLETE));
                    oldState = tip.setState(TaskState.COMPLETE);
                } else {
                    // otherwise, we are done.
                    oldState = tip.setState(state);
                }
                availMapOutput.unsetTakeOver(tip.getPartition());
            } else {
                oldState = tip.setState(state);
                if ( state == TaskState.PREPARE_TAKEOVER ) {
                    availMapOutput.setTakeOver(tip.getPartition());
                }
                
            }
            if ( oldState != state ) {
                this.newTakeOver.add(new TaskStatusEvent(tip.getTaskID(),state));
            }
        } else {
            TaskState oldState = tip.setState(state); // for reduce, set and forget it
            if ( oldState != TaskState.RUNNING ) {
                LOG.warn("Invalied state. expect RUNNING but "+oldState);
            }
        }
//      TaskState oldState = tip.setState(state);
//      if ( state != oldState && tip.getTaskType() == TaskType.MAP ) {
//          this.newTakeOver.add(new TaskStatusEvent(tip.getTaskID(),state));
//      }
    }
    
    public synchronized boolean isAllMapOutputIndexAvailable() {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace("reactive map "+job.getJobID()+" # maps = "+numMaps+" available = "+availMapOutIndex.cardinality()+" "+availMapOutIndex);
        }
        
        return this.availMapOutIndex.cardinality() == numMaps;
    }
    
    public synchronized boolean isCompleted() {
        return completedTasks.cardinality() == (this.numMaps + this.numReduces);
    }
    
    public synchronized void getNumberOfReservedTasks(int[] counts) {
        // the number of tasks which are either running or not scheduled yet.
        if ( isCompleted() ) return; // nothing is reserved
        
        int inCompleteMaps = numMaps;
        for ( int i = 0; i < numMaps; ++i ) {
            TaskState state = tasks[i].getState();
            if ( state == TaskState.COMPLETE || state == TaskState.TAKEOVER ) {
                --inCompleteMaps;
            }
        }
        counts[0] += inCompleteMaps;

        int inCompleteReduces = numReduces;
        for ( int i = numMaps; i < tasks.length; ++i ) {
            TaskState state = tasks[i].getState();
            if ( state == TaskState.COMPLETE || state == TaskState.TAKEOVER ) {
                // FIXME should be more precise. if handover happened, it must be confirmed by task completion message
                --inCompleteReduces;
            }
        }
        counts[1] += inCompleteReduces;
    }
    
    public float getAverageTimePerByte(TaskType type,float defaultValue) {
        boolean isMap = type == TaskType.MAP;
        Average avg = isMap ? mapAvgTimePerByte : reduceAvgTimePerByte;
        float v = (float)avg.get();
        if ( v == 0. ) {
            avg = new Average();
            int i = isMap ? 0 : numMaps;
            int end = isMap ? numMaps : numMaps+numReduces;
            
            for ( ; i < end; ++i ) {
                tasks[i].getTimePerByte(avg);
            }
            
            v = (float)avg.get();
            
            if ( Float.isNaN(v) ) {
                LOG.warn("average time per byte is NaN! force reset to 0");
                v = 0.f;
            }
        }
        return v == 0.f ? defaultValue : v;
    }
    

    /**
     * by default, it's all task tracker output.
     * @param taskid
     * @return
     * @throws IOException
     * @throws InterruptedException 
     * @throws ExecutionException 
     * @throws  
     * @throws ExecutionException 
     */
    public MapOutputSplitInfo getRecursiveMapOutputSplits(TaskID taskid)
            throws IOException, InterruptedException {
        TaskCompletionEvent[] events = null;
        
        boolean good = false;
        if ( eventCompletionEvents == null ) {
            throw new IllegalStateException("map output completion events haven't been loaded for "+taskid);
        } else if ( eventCompletionEvents.isDone() ) {
            try {
                good = eventCompletionEvents.get(); // will throw exception or mark it good
            } catch ( ExecutionException ex ) {
                throw new IOException(ex);
            }
        } else {
            // should wait
            try {
                good = eventCompletionEvents.get(10,TimeUnit.SECONDS);
            } catch ( TimeoutException toex ) {
                LOG.fatal("taking too long to load map completion events for "+taskid);
            } catch (ExecutionException e) {
                throw new IOException(e);
            }
        }

        if ( ! good ) {
            throw new IllegalStateException("Failed to load all map completion events for "+taskid);
        }

        synchronized (completedMapEvents) {
            events = completedMapEvents.toArray(EMPTY_EVENTS);
            if (completedMapEvents.size() < numMaps) {
                throw new IllegalStateException(String.format(
                        "only %d out of %d map outputs are available for %s!",
                        events.length, numMaps, getJobID()));
            }
        }
        
        HashMap<String, MapOutputSplit> splits = new HashMap<String, MapOutputSplit>();

        for (TaskCompletionEvent event : events) {
            String uri = event.getTaskTrackerHttp();
            String host = extractHost(uri);
            MapOutputSplit split = splits.get(host);
            if (split == null) {
                split = new MapOutputSplit(host, uri);
                splits.put(host, split);
            }
            split.add(event.getTaskAttemptId());
        }
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Total " + splits.size() + " tasktracker splits for " + taskid);
        }
        return new MapOutputSplitInfo(splits.size(),new ArrayList<MapOutputSplit>(splits.values()));
    }
    
    /**
     * 
     * @param oldOne  JIP structure of the bottleneck job
     * @param taskid  task id of the bottlenect task
     * @param newOne  JIP structure of the mitigation job
     * @param nSplits revised number of tasks
     * @throws IOException
     * @throws InterruptedException
     */

    @SuppressWarnings("unchecked")
    private static void setupRecursiveReactiveReduce(JobInProgress oldOne,
            TaskID taskid, JobInProgress newOne, int nSplits)
            throws IOException, InterruptedException {
        setupReactiveJob(oldOne, taskid, newOne);

        // setup configurations for reduce
        // reduce needs to retrieve output from original job.

        // wait until token is loaded
        TokenStorage oldTokenStorage = oldOne.getTokenStorage(true);
        Token<JobTokenIdentifier> jt = (Token<JobTokenIdentifier>) TokenCache.getJobToken(oldTokenStorage);
        if (LOG.isDebugEnabled()) {
            LOG.debug(oldOne.getJobID().toString() + " job token password = " + Arrays.toString(jt.getPassword()));
        }

        newOne.getTokenStorage().addToken(new Text(oldOne.getJobID().toString()), jt);

        if (LOG.isDebugEnabled()) {
            LOG.debug(oldOne.getJobID() + " password = "
                    + Arrays.toString(jt.getPassword()));
            LOG.debug(oldOne.getJobID()
                    + " secret = "
                    + Arrays.toString(JobTokenSecretManager.createSecretKey(
                            jt.getPassword()).getEncoded()));
        }

        final Job newJob = newOne.getJob();
        final Configuration conf = newJob.getConfiguration();
        final Configuration oldConf = oldOne.getConfiguration();

        newJob.setTokenStorage(newOne.getTokenStorage());

        // we need a new input format

        // we need to check the API because of the combiner and partitioner
        // interface

        boolean useNewApi = conf.getBoolean("mapred.reducer.new-api", false);

        // do we have a special partitioner?
        final int numSplits = nSplits == 0 ? 2 : nSplits;

        if (useNewApi) {
            newJob.setInputFormatClass(MapOutputInputFormat.class);
            newJob.setMapperClass(Mapper.class);
            
            Utils.copyConfiguration(oldConf, conf, ORIGINAL_PARTITIONER_CLASS_ATTR);
        } else {
            conf.setClass("mapred.input.format.class",
                    MapOutputInputFormat.class,
                    org.apache.hadoop.mapred.InputFormat.class);
            conf.setClass("mapred.mapper.class",
                    org.apache.hadoop.mapred.lib.IdentityMapper.class,
                    org.apache.hadoop.mapred.Mapper.class);
            
            Utils.copyConfiguration(oldConf, conf, ORIGINAL_PARTITIONER_CLASS_ATTR);

            conf.setBoolean("mapred.mapper.new-api", false); // force
            conf.setBoolean("mapred.reducer.new-api", false);
        }

        // setup output directory. output format is identical.
        // Path orgOutDir =
        // FileOutputFormat.getOutputPath(oldOne.getOriginalJob().getJob());
        Path orgOutDir = oldOne.getOriginalJob().getOutputPath();
        Path oldPath = oldOne.getOutputPath();
        String oldName = oldPath.getName();
        
        // consult the schedule order to check actual key order
        int realPartition = taskid.getId();
        if ( oldOne.plan != null && oldOne.plan.getScheduleOrder() != null ) {
        	realPartition = oldOne.plan.getScheduleOrder()[realPartition];
        }
        
        Path outDir = new Path(orgOutDir, getRecursiveOutputPathName(oldName, realPartition, oldOne.getNumReduceTasks()));
        FileOutputFormat.setOutputPath(newJob, outDir);

        FileSystem fs = outDir.getFileSystem(conf);
        if (fs.exists(outDir)) {
            fs.delete(outDir, true);
        }

        // FIXME depending on new API or old API, change the mapper class

        if (conf.get(COMBINE_CLASS_ATTR) != null) {
            // FIXME bump up spill size. we know that the mapper does not
            // require much memory. what if we need more complex partition
            // function?
            // for example, what if we have to see all the data to evenly
            // partition the input?
            // conf.setFloat(MAP_SORT_SPILL_PERCENT,0.0f); // 0.8 by default
        }

        // FIXME
        conf.setInt(IO_SORT_MB, 256); // just assign enough memory since we
                                      // know that the map does nothing

        conf.setInt(ORIGINAL_NUM_REDUCES, newJob.getNumReduceTasks());
        conf.setBoolean(SKEWREDUCE_ENABLE_REACTIVE_SHUFFLE, false); // disable reactive shuffle -- we don't need it!

        // FIXME determine the number of reducers
        newJob.setNumReduceTasks(numSplits);
        
        // lastly check whether the input split has been loaded
        
        if ( useNewApi ) { // delay loading
            List<MapOutputSplit> splits = oldOne.getRecursiveMapOutputSplits(taskid).splits;
            InputSplitCache.set(conf, splits);
        } else {
            List<MapOutputSplit> splits = oldOne.getRecursiveMapOutputSplits(taskid).splits;
            InputSplitCache.set(conf, splits);
        }
    }
}
