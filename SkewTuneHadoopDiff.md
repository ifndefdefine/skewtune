

# Overview #

SkewTune is based on Hadoop 0.21.0. We tried to minimize the
changes to the core Hadoop services (i.e., the trackers) thus most of changes
are made to the code running in the user process that spawned by the task
tracker.

# Changes #

## HDFS ##

  * HDFS data node is updated to handle a large block size (> 2GB). The change is to store the large map output of the mitigated task so that the map output can be read by a sequential scan from a single disk.

## MapReduce ##

Most of the changes are made to the code that runs in the user process.

### Job Tracker ###

  * The only change is in the JobInProgress class. The change tracks dependencies between jobs and do not delete map output until all dependant subjobs completed.

### MapReduce ###

### Task ###

  * Child, Task, MapTask, and ReduceTask classes are modified to setup extra communication channels to the SkewTune trackers.
  * The communication thread reports to both trackers (i.e., Hadoop and SkewTune).
  * TaskProgress implements more sophisticated task progress estimator.

#### Map Phase ####

  * If a job is a map-only job, it is possible to sort the map output as if there existed reduce tasks. This is to mitigate a map task. The output of the job is written to the HDFS for fault-tolerance and load-balance so that multiple reducers can read the data concurrently without overloading particular data node.

#### Shuffle Phase ####
  * Shuffle phase can handle both a regular map output from the task tracker and a HDFS file produced by a mitigated map task.
  * If a map task is mitigated, the mitigated map output is transferred via inter-tracker and umbilical protocols.
  * Maximum heap size to buffer map output in memory is patched to avoid deadlock.

#### Reduce Phase ####
  * The ValueIterator class has modified to implement stop and scan remaining input data on request.

### InputFormat ###
  * A new record reader must implement SplittableRecordReader and ScannableReader interfaces to support mitigation.