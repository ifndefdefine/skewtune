

This wiki describes the important configuration parameters for the SkewTune.

# SkewTune Job Tracker #

| **Field** | **Description** | **Default** |
|:----------|:----------------|:------------|
|skewtune.jobtracker.address| IPC address of job tracker | 0.0.0.0:60000 |
|skewtune.jobtracker.http.address| HTTP address of job tracker (not supported) | 0.0.0.0:60050 |
|skewtune.jobtracker.handler.count| Number of IPC handler threads of job tracker| 40 |

# SkewTune Task Tracker #

| **Field** | **Description** | **Default** |
|:----------|:----------------|:------------|
|skewtune.tasktracker.report.address| Address that the task tracker listening to user tasks |127.0.0.1:60100|

# SkewTune Job #

## Enable/Disable SkewTune ##
| **Field** | **Default** | **Description** |
|:----------|:------------|:----------------|
|skewtune.job.enable| false | Enable map and reduce mitigation. |
|skewtune.map.enable | false | Enable map mitigation. |
|skewtune.reduce.enable| false | Enable reduce mitigation. |
|skewtune.job.takeover.enable| true | When false, re-execute map and reduce straggler rather. In other words, the mitigation job re-process the input data but with more parallelism.|
|skewtune.map.takeover.enable| true | When false, re-execute map straggler. |
|skewtune.reduce.takeover.enable| true | When false, re-execute reduce stragger. |

## Internal Configuration ##
| **Field** | **Default** | **Description** |
|:----------|:------------|:----------------|
|skewtune.job.map.sort.output| false | When true, always sort map output.|
|skewtune.job.map.replication| 5 | Replication factor of map output.|
|skewtune.job.split.default| 2 | Set default number of splits on repartitioning. |
|skewtune.job.map.split.default| 2 | Set default number of splits on repartitioning a map task.|
|skewtune.job.reactive.partitioner.class|  | If set, SkewTune will use given class to repartition map output.  |
|skewtune.job.delegatepartitioner.reduces|  | If the original job uses hash partitioning, this attribute specifies the new number of reduce tasks after repartition |
|skewtune.job.map.split|  |  |
|skewtune.job.map.split.minsize|  |  |
|skewtune.job.reactive.reduce.minKey|  | The lastly processed key. Excluded in this mitigation job.|
|skewtune.job.reactive.reduce.maxKey|  | The greatest key to process. Included in this mitigation job.|
|skewtune.job.reactive| false | **true** if this is a mitigation job |
|skewtune.job.doNotSpeculate|  | only for experiment. |
|skewtune.partition.sample.interval|  | Sampling record/reduce key interval in bytes.|
|skewtune.job.takeover.repartition|  |  |
|skewtune.partition.bucket|  | Range information for the repartitioning reduce input.|
|skewtune.partition.schedule|  | Mapping from scheduling order to key order.|
|skewtune.partition.sample.numSlots| 20 | The number of slots available in the cluster. Or, the number of slots that can run the mitigation tasks. |
|skewtune.partition.sample.factor| 1 | Larger value will return more sample.|
|skewtune.partition.sample.minSamplePerSlot| 10 | The minimum number of samples per slot. |
|skewtune.scan.diskbw| 30 | Disk read bandwidthin MB/s. |
|skewtune.scan.maxTime| 60 | Set time limit on local scan. The task will do local scan if it could be done within the time limit. |
|skewtune.scan.localScan.force|  | when true, SkewTune always perform local scan on mitigation.|
|skewtune.reduce.shuffle.reactive| false | Enable or disable shuffle reactive map output.  true only for the original reduce tasks. map tasks of reactive reduce job do not split because we assume that they are already small.  |

## Configuration of the Original Job ##

| **Field** | **Default** | **Description** |
|:----------|:------------|:----------------|
|skewtune.job.original.jobid|  | Job ID of the original job if this job is a mitigation job.|
|skewtune.job.original.taskid|  | Task ID of the original task that this job mitigates.|
|skewtune.job.original.reduces|  | The number of reduce tasks of the original job.|
|skewtune.job.original.inputformat.class|  | InputFormat class of the original job.|
|skewtune.job.original.partitioner.class|  | Partitioner class of the original job.|
|skewtune.job.original.end-notification.url|  | Job end notification URL of the original job.|
|skewtune.job.original.task.http|  | Hadoop job tracker|
|skewtune.job.original.map.output.key.class|  | Map output key class of the original job.|
|skewtune.job.original.map.output.key.comparator.class|  | Map output key comparator class of the original job.|
|skewtune.job.original.output.group.comparator.class|  | Reduce key group comparator class of the original job.|
