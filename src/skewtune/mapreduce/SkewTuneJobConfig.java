package skewtune.mapreduce;

public interface SkewTuneJobConfig {
    public static final String ENABLE_SKEWTUNE = "skewtune.job.enable";
    public static final String ENABLE_SKEWTUNE_MAP = "skewtune.map.enable";
    public static final String ENABLE_SKEWTUNE_REDUCE = "skewtune.reduce.enable";
    
    public static final String ENABLE_SKEWTUNE_TAKEOVER = "skewtune.job.takeover.enable";
    public static final String ENABLE_SKEWTUNE_TAKEOVER_MAP = "skewtune.map.takeover.enable";
    public static final String ENABLE_SKEWTUNE_TAKEOVER_REDUCE = "skewtune.reduce.takeover.enable";

    public static final String MAP_OUTPUT_SORT_ATTR = "skewtune.job.map.sort.output";
    public static final String EXTRA_PARTITIONER_ATTR = "skewtune.job.partitioner.class";
    public static final String MAP_OUTPUT_REPLICATION_ATTR = "skewtune.job.map.replication";

    
    public static final String DEFAULT_NUM_SPLITS = "skewtune.job.split.default";
    public static final String DEFAULT_NUM_MAP_SPLITS = "skewtune.job.map.split.default";
    public static final String DEFAULT_NUM_REDUCE_SPLITS = "skewtune.job.reduce.split.default";
    
    public static final String DEFAULT_MIN_SPLIT_SIZE = "skewtune.job.split.minsize";
    public static final String DEFAULT_MIN_MAP_SPLIT_SIZE = "skewtune.job.map.split.minsize";
    public static final String DEFAULT_MIN_REDUCE_SPLIT_SIZE = "skewtune.job.reduce.split.minsize";

    public static final String REACTIVE_PARTITIONER_CLASS_ATTR = "skewtune.job.reactive.partitioner.class";
    public static final String DELEGATE_PARTITIONER_NUM_VIRTUAL_REDUCES = "skewtune.job.delegatepartitioner.reduces";
    
    public static final String SKEWTUNE_JT_HTTP_ATTR = "skewtune.jobtracker.http";
    
    public static final String MAP_INPUT_MONITOR_URL_ATTR = "skewtune.job.map.input-monitor.url";
    public static final String MAP_NUM_SPLITS_ATTR = "skewtune.job.map.split";
    public static final String MAP_MIN_SPLIT_SIZE_ATTR = "skewtune.job.map.split.minsize";
    
    /**
     * exclusive
     */
    public static final String REACTIVE_REDUCE_MIN_KEY  = "skewtune.job.reactive.reduce.minKey";
    /**
     * inclusive
     */
    public static final String REACTIVE_REDUCE_MAX_KEY  = "skewtune.job.reactive.reduce.maxKey";
    
    public static final String SKEWTUNE_REACTIVE_JOB = "skewtune.job.reactive"; // true if this is reactive job

    public static final String SKEWTUNE_DO_NOT_SPECULATE = "skewtune.job.doNotSpeculate"; // for experiments

    
    // run scan and plan before launching reaction
    public static final String SKEWTUNE_REPARTITION_SAMPLE_INTERVAL = "skewtune.partition.sample.interval";
    public static final String SKEWTUNE_REPARTITION_STRATEGY = "skewtune.job.takeover.repartition";
    public static final String SKEWTUNE_REPARTITION_BUCKETS = "skewtune.partition.bucket";
    public static final String SKEWTUNE_REPARTITION_SCHEDULES = "skewtune.partition.schedule";
    public static final String SKEWTUNE_REPARTITION_SAMPLE_NUM_SLOTS = "skewtune.partition.sample.numSlots";
    public static final String SKEWTUNE_REPARTITION_SAMPLE_FACTOR = "skewtune.partition.sample.factor";
    public static final String SKEWTUNE_REPARTITION_MIN_SAMPLE_PER_SLOT = "skewtune.partition.sample.minSamplePerSlot";
    public static final String SKEWTUNE_REPARTITION_DISK_BW = "skewtune.scan.diskbw";
    public static final String SKEWTUNE_REPARTITION_MAX_SCAN_TIME = "skewtune.scan.maxTime";
    public static final String SKEWTUNE_REPARTITION_LOCALSCAN_FORCE = "skewtune.scan.localScan.force";

    public static final String SKEWTUNE_DEBUG_GENERATE_DISKLOAD_FOR_MAP = "skewtune.debug.diskLoad.map";
    public static final String SKEWTUNE_DEBUG_GENERATE_DISKLOAD_FOR_REDUCE = "skewtune.debug.diskLoad.reduce";
    public static final String SKEWTUNE_DEBUG_GENERATE_DISKLOAD_CMD = "skewtune.debug.diskLoad.cmd";
    public static final String SKEWTUNE_DEBUG_GENERATE_DISKLOAD_AMOUNT = "skewtune.debug.diskLoad.amount";
    
    public static final String SKEWTUNE_DEBUG_REDUCE_DROP_DISKCACHE = "skewtune.debug.reduce.dropDiskCache";
    
    /**
     * enable or disable shuffle reactive map output.
     * true only for the original reduce tasks. map tasks of reactive reduce job do not split because
     * we assume that they are already small.
     */
    public static final String SKEWREDUCE_ENABLE_REACTIVE_SHUFFLE = "skewtune.reduce.shuffle.reactive";
    
    // KEEP TRACK OF ORIGINAL CONFIGURATION
    
    public static final String ORIGINAL_JOB_ID_ATTR = "skewtune.job.original.jobid";
    public static final String ORIGINAL_TASK_ID_ATTR = "skewtune.job.original.taskid";
    public static final String ORIGINAL_NUM_REDUCES = "skewtune.job.original.reduces";
    public final static String ORIGINAL_INPUT_FORMAT_CLASS_ATTR = "skewtune.job.original.inputformat.class";
    public final static String ORIGINAL_PARTITIONER_CLASS_ATTR = "skewtune.job.original.partitioner.class";
    public final static String ORIGINAL_END_NOTIFICATION_URL = "skewtune.job.original.end-notification.url";
    public final static String ORIGINAL_TASK_TRACKER_HTTP_ATTR = "skewtune.job.original.task.http";
    
    public static final String ORIGINAL_MAP_OUTPUT_KEY_CLASS = "skewtune.job.original.map.output.key.class";
    public static final String ORIGINAL_MAP_OUTPUT_KEY_COMPARATOR_CLASS = "skewtune.job.original.map.output.key.comparator.class";
    public static final String ORIGINAL_GROUP_COMPARATOR_CLASS = "skewtune.job.original.output.group.comparator.class";
}
