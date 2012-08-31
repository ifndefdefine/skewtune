/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.task.reduce.Shuffle;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.SkewTuneJobConfig;
import skewtune.mapreduce.protocol.STTaskStatus;
import skewtune.mapreduce.protocol.SkewTuneTaskUmbilicalProtocol;
import skewtune.utils.LoadGen;

/** A Reduce task. */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReduceTask extends Task {

  static {                                        // register a ctor
    WritableFactories.setFactory
      (ReduceTask.class,
       new WritableFactory() {
         public Writable newInstance() { return new ReduceTask(); }
       });
  }
  
  private static final Log LOG = LogFactory.getLog(ReduceTask.class.getName());
  private int numMaps;

  private CompressionCodec codec;


  { 
    getProgress().setStatus("reduce"); 
    setPhase(TaskStatus.Phase.SHUFFLE);        // phase to start with 
  }

  private Progress copyPhase;
  private Progress sortPhase;
  private Progress reducePhase;
  private Counters.Counter shuffledMapsCounter = 
    getCounters().findCounter(TaskCounter.SHUFFLED_MAPS);
  private Counters.Counter reduceShuffleBytes = 
    getCounters().findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES);
  private Counters.Counter reduceInputKeyCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_INPUT_GROUPS);
  private Counters.Counter reduceInputValueCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_INPUT_RECORDS);
  private Counters.Counter reduceOutputCounter = 
    getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);
  private Counters.Counter reduceCombineInputCounter =
    getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
  private Counters.Counter reduceCombineOutputCounter =
    getCounters().findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
  
  // SKEWREDUCE
  private Counters.Counter reduceInputBytes =
    getCounters().findCounter(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.COUNTER_GROUP,org.apache.hadoop.mapreduce.lib.input.FileInputFormat.BYTES_READ);

  // A custom comparator for map output files. Here the ordering is determined
  // by the file's size and path. In case of files with same size and different
  // file paths, the first parameter is considered smaller than the second one.
  // In case of files with same size and path are considered equal.
  private Comparator<FileStatus> mapOutputFileComparator = 
    new Comparator<FileStatus>() {
      public int compare(FileStatus a, FileStatus b) {
        if (a.getLen() < b.getLen())
          return -1;
        else if (a.getLen() == b.getLen())
          if (a.getPath().toString().equals(b.getPath().toString()))
            return 0;
          else
            return -1; 
        else
          return 1;
      }
  };
  
  // A sorted set for keeping a set of map output files on disk
  private final SortedSet<FileStatus> mapOutputFilesOnDisk = 
    new TreeSet<FileStatus>(mapOutputFileComparator);

  public ReduceTask() {
    super();
  }

  public ReduceTask(String jobFile, TaskAttemptID taskId,
                    int partition, int numMaps, int numSlotsRequired) {
    super(jobFile, taskId, partition, numSlotsRequired);
    this.numMaps = numMaps;
  }
  
  private CompressionCodec initCodec() {
    // check if map-outputs are to be compressed
    if (conf.getCompressMapOutput()) {
      Class<? extends CompressionCodec> codecClass =
        conf.getMapOutputCompressorClass(DefaultCodec.class);
      return ReflectionUtils.newInstance(codecClass, conf);
    } 

    return null;
  }

  @Override
  public TaskRunner createRunner(TaskTracker tracker, TaskInProgress tip) 
  throws IOException {
    return new ReduceTaskRunner(tip, tracker, this.conf);
  }

  @Override
  public boolean isMapTask() {
    return false;
  }

  public int getNumMaps() { return numMaps; }
  
  /**
   * Localize the given JobConf to be specific for this task.
   */
  @Override
  public void localizeConfiguration(JobConf conf) throws IOException {
    super.localizeConfiguration(conf);
    conf.setNumMapTasks(numMaps);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeInt(numMaps);                        // write the number of maps
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    numMaps = in.readInt();
  }
  
  // Get the input files for the reducer.
  private Path[] getMapFiles(FileSystem fs, boolean isLocal) 
  throws IOException {
    List<Path> fileList = new ArrayList<Path>();
    if (isLocal) {
      // for local jobs
      for(int i = 0; i < numMaps; ++i) {
        fileList.add(mapOutputFile.getInputFile(i));
      }
    } else {
      // for non local jobs
      for (FileStatus filestatus : mapOutputFilesOnDisk) {
        fileList.add(filestatus.getPath());
      }
    }
    return fileList.toArray(new Path[0]);
  }

  private class ReduceValuesIterator<KEY,VALUE> 
          extends ValuesIterator<KEY,VALUE> {
      protected ReduceValuesIterator() {}
      
    public ReduceValuesIterator (RawKeyValueIterator in,
                                 RawComparator<KEY> comparator, 
                                 Class<KEY> keyClass,
                                 Class<VALUE> valClass,
                                 Configuration conf, Progressable reporter)
      throws IOException {
      super(in, comparator, keyClass, valClass, conf, reporter);
    }

    @Override
    public VALUE next() {
      reduceInputValueCounter.increment(1);
      return moveToNext();
    }
    
    protected VALUE moveToNext() {
      return super.next();
    }
    
    public void informReduceProgress() {
      reducePhase.set(super.in.getProgress().getProgress()); // update progress
      reporter.progress();
    }
  }

  private class SkippingReduceValuesIterator<KEY,VALUE> 
     extends ReduceValuesIterator<KEY,VALUE> {
     private SkipRangeIterator skipIt;
     private TaskUmbilicalProtocol umbilical;
     private Counters.Counter skipGroupCounter;
     private Counters.Counter skipRecCounter;
     private long grpIndex = -1;
     private Class<KEY> keyClass;
     private Class<VALUE> valClass;
     private SequenceFile.Writer skipWriter;
     private boolean toWriteSkipRecs;
     private boolean hasNext;
     private TaskReporter reporter;
     
     public SkippingReduceValuesIterator(RawKeyValueIterator in,
         RawComparator<KEY> comparator, Class<KEY> keyClass,
         Class<VALUE> valClass, Configuration conf, TaskReporter reporter,
         TaskUmbilicalProtocol umbilical) throws IOException {
       super(in, comparator, keyClass, valClass, conf, reporter);
       this.umbilical = umbilical;
       this.skipGroupCounter = 
         reporter.getCounter(TaskCounter.REDUCE_SKIPPED_GROUPS);
       this.skipRecCounter = 
         reporter.getCounter(TaskCounter.REDUCE_SKIPPED_RECORDS);
       this.toWriteSkipRecs = toWriteSkipRecs() &&  
         SkipBadRecords.getSkipOutputPath(conf)!=null;
       this.keyClass = keyClass;
       this.valClass = valClass;
       this.reporter = reporter;
       skipIt = getSkipRanges().skipRangeIterator();
       mayBeSkip();
     }
     
     public void nextKey() throws IOException {
       super.nextKey();
       mayBeSkip();
     }
     
     public boolean more() { 
       return super.more() && hasNext; 
     }
     
     private void mayBeSkip() throws IOException {
       hasNext = skipIt.hasNext();
       if(!hasNext) {
         LOG.warn("Further groups got skipped.");
         return;
       }
       grpIndex++;
       long nextGrpIndex = skipIt.next();
       long skip = 0;
       long skipRec = 0;
       while(grpIndex<nextGrpIndex && super.more()) {
         while (hasNext()) {
           VALUE value = moveToNext();
           if(toWriteSkipRecs) {
             writeSkippedRec(getKey(), value);
           }
           skipRec++;
         }
         super.nextKey();
         grpIndex++;
         skip++;
       }
       
       //close the skip writer once all the ranges are skipped
       if(skip>0 && skipIt.skippedAllRanges() && skipWriter!=null) {
         skipWriter.close();
       }
       skipGroupCounter.increment(skip);
       skipRecCounter.increment(skipRec);
       reportNextRecordRange(umbilical, grpIndex);
     }
     
     @SuppressWarnings("unchecked")
     private void writeSkippedRec(KEY key, VALUE value) throws IOException{
       if(skipWriter==null) {
         Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
         Path skipFile = new Path(skipDir, getTaskID().toString());
         skipWriter = SequenceFile.createWriter(
               skipFile.getFileSystem(conf), conf, skipFile,
               keyClass, valClass, 
               CompressionType.BLOCK, reporter);
       }
       skipWriter.append(key, value);
     }
  }
  
    private class SplittableReduceValuesIterator<KEY, VALUE> extends
            ReduceValuesIterator<KEY, VALUE> {
        final ReduceValuesIterator<KEY,VALUE> values;
        boolean notSplitted;
        boolean processing;
        boolean runLastMore;
        // capture current key
        // serializer?
        
        
        public SplittableReduceValuesIterator(ReduceValuesIterator<KEY,VALUE> values) {
            this.values = values;
            this.notSplitted = true;
        }

        @Override
        public VALUE next() {
            // should be incremented in wrapped iterators
//            reduceInputValueCounter.increment(1);
            return moveToNext();
        }

        @Override
        protected VALUE moveToNext() {
            return values.next();
        }

        @Override
        public void informReduceProgress() {
            values.informReduceProgress();
        }

        @Override
        RawKeyValueIterator getRawIterator() {
            return values.getRawIterator();
        }

        @Override
        public boolean hasNext() {
            return values.hasNext();
        }

        @Override
        public void remove() {
            values.remove();
        }

        @Override
        public KEY getKey() {
            return values.getKey();
        }

        @Override
        public synchronized void nextKey() throws IOException {
            if ( notSplitted ) {
                values.nextKey();
            } // otherwise, we do not need to move to nextKey()
            processing = false; // getKey() is not processed yet
        }

        @Override
        public synchronized boolean more() {
            boolean cont = false;
            if ( notSplitted ) {
                cont = values.more();
            } else if ( runLastMore ) {
                cont = values.more();
                runLastMore = false;
            }
            processing = true;
            return cont;
        }
        
        public synchronized StopStatus tellAndStop(Serializer serializer) throws IOException {
            notSplitted = false;
            serializer.serialize(getKey());
            if ( processing ) {
                // we're good. currently processed key has been captured.
            } else {
                // the following more() should continue.and process the remaining
                runLastMore = true;
            }
            return StopStatus.STOPPED;
        }
    }


  @Override
  @SuppressWarnings("unchecked")
  public void run(JobConf job, final TaskUmbilicalProtocol umbilical, final SkewTuneTaskUmbilicalProtocol srumbilical)
    throws IOException, InterruptedException, ClassNotFoundException {
    this.umbilical = umbilical;
    this.srumbilical = srumbilical;

    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());

    if (isMapOrReduce()) {
      copyPhase = getProgress().addPhase("copy");
      sortPhase  = getProgress().addPhase("sort");
      reducePhase = getProgress().addPhase("reduce");
    }
    // start thread that will handle communication with parent
    myProgress = new TaskProgress.ReduceProgress(job, getCounters(), taskStatus, copyPhase, sortPhase);
    TaskReporter reporter = startReporter(umbilical,srumbilical,myProgress);
    
    boolean useNewApi = job.getUseNewReducer();
    initialize(job, getJobID(), reporter, useNewApi);

    // check if it is a cleanupJobTask
    if (jobCleanup) {
      runJobCleanupTask(umbilical, reporter);
      return;
    }
    if (jobSetup) {
      runJobSetupTask(umbilical, reporter);
      return;
    }
    if (taskCleanup) {
      runTaskCleanupTask(umbilical, reporter);
      return;
    }

    // almost there.
    if ( job.getBoolean(SkewTuneJobConfig.ENABLE_SKEWTUNE_REDUCE, false) && srumbilical != null ) {
        // okay, time to enable monitoring
        try {
            srumbilical.init(getTaskID(),job.getNumMapTasks(), job.getNumReduceTasks());
            this.srTaskStatus = new STTaskStatus(this.getTaskID());
//            myProgress = new TaskProgress.ReduceProgress(job, reporter, taskStatus, copyPhase, sortPhase);
//            srTaskStatus.setStartTime(System.currentTimeMillis());
            reportSkewReduce = true;
            LOG.info("enabling skewreduce monitoring");
        } catch ( IOException e ) {
            LOG.warn("unable to contact skewreduce tracker",e);
        }
    }
    
    // Initialize the codec
    codec = initCodec();
    RawKeyValueIterator rIter = null;
    boolean isLocal = "local".equals(job.get(JTConfig.JT_IPC_ADDRESS, "local"));
    
    SortStopAction sortStopAction = null;
    
    if ( myProgress != null ) {
        myProgress.beginTask();
    }
    
    if (!isLocal) {
      Class combinerClass = conf.getCombinerClass();
      CombineOutputCollector combineCollector = 
        (null != combinerClass) ? 
            new CombineOutputCollector(reduceCombineOutputCounter) : null;
            
      boolean reactiveShuffle = reportSkewReduce && job.getBoolean(SkewTuneJobConfig.SKEWREDUCE_ENABLE_REACTIVE_SHUFFLE, true);

      Shuffle shuffle = 
        new Shuffle(getTaskID(), job, FileSystem.getLocal(job), umbilical, 
                    super.lDirAlloc, reporter, codec, 
                    combinerClass, combineCollector, 
                    spilledRecordsCounter, reduceCombineInputCounter,
                    shuffledMapsCounter,
                    reduceShuffleBytes, failedShuffleCounter,
                    mergedMapOutputsCounter,
                    taskStatus, copyPhase, sortPhase, this,
                    srumbilical,
//                    reportSkewReduce,
                    reactiveShuffle,
                    myProgress);
      
      if ( reportSkewReduce ) {
          sortStopAction = new SortStopAction();
          if ( ! setupStopHandler(sortStopAction) ) {
              LOG.warn("this task has been handovered!");
              throw new InterruptedIOException();
          }
      }
          
      try {
          rIter = shuffle.run();
      } catch ( IOException ex ) {
          if ( ex instanceof InterruptedIOException ) {
              // FIXME interrupted by CANCEL or STOP request
              // if it is stopped, we should send empty request
              if ( sortStopAction != null ) {
                  LOG.info("SORT has been interrupted!");
                  sortStopAction.setStopWithEmptyResponse();
              }
          } else {
              throw ex;
          }
      } catch ( InterruptedException ex ) { // during shuffle phase
          if ( sortStopAction != null ) {
              LOG.info("SHUFFLE has been interrupted!");
              sortStopAction.setStopWithEmptyResponse();
          }
      }
    } else {
      final FileSystem rfs = FileSystem.getLocal(job).getRaw();
      rIter = Merger.merge(job, rfs, job.getMapOutputKeyClass(),
                           job.getMapOutputValueClass(), codec, 
                           getMapFiles(rfs, true),
                           !conf.getKeepFailedTaskFiles(), 
                           job.getInt(JobContext.IO_SORT_FACTOR, 100),
                           new Path(getTaskID().toString()), 
                           job.getOutputKeyComparator(),
                           reporter, spilledRecordsCounter, null, null);
    }
    
    // free up the data structures
    mapOutputFilesOnDisk.clear();
    
    sortPhase.complete();                         // sort is complete
    
    clearStopHandler(TaskStatus.Phase.REDUCE);
//    setPhase(TaskStatus.Phase.REDUCE);
    statusUpdate(umbilical);
    
    Class keyClass = job.getMapOutputKeyClass();
    Class valueClass = job.getMapOutputValueClass();
    RawComparator comparator = job.getOutputValueGroupingComparator();

    // FIXME setup time for LATE
    if ( myProgress != null ) {
        myProgress.beginSetup();
    }
    
    boolean forceLocalScan = conf.getBoolean(SkewTuneJobConfig.SKEWTUNE_REPARTITION_LOCALSCAN_FORCE,false);
    if ( forceLocalScan ) {
        if ( conf.getBoolean(SkewTuneJobConfig.SKEWTUNE_DEBUG_GENERATE_DISKLOAD_FOR_REDUCE, false) ) {
            LoadGen diskLoad = new LoadGen(conf);
            diskLoad.start();
            Thread.sleep(10000); // let other processes start
        }
        
        long bytes = myProgress.getRemainingBytes(null);
        StopContext localStop = new StopContext(conf);
        localStop.setRemainBytes(bytes);
        LOG.warn("this task is running local scan experiment.");
        LOG.info("initiaiting scan = "+bytes+" bytes. sample interval = "+localStop.getSampleInterval());
        new SortStopAction(localStop).scan(job, rIter, reporter, comparator, keyClass, valueClass);
    } else if ( sortStopAction == null || ! sortStopAction.isInterrupted() ) {
        if (useNewApi) {
          runNewReducer(job, umbilical, reporter, rIter, comparator, keyClass, valueClass);
        } else if ( reportSkewReduce ) {
          runSkewTuneOldReducer(job, umbilical, reporter, rIter, comparator, keyClass, valueClass);
        } else {
          runOldReducer(job, umbilical, reporter, rIter, comparator, keyClass, valueClass);
        }
    } else {
        LOG.warn("this task has been handovered. bypassing reduce phase.");
        
        if ( ! sortStopAction.isDone() ) {
            LOG.info("initiaiting scan");
            sortStopAction.scan(job, rIter, reporter, comparator, keyClass, valueClass);
        }
    }

    if ( myProgress != null ) {
        myProgress.endTask();
    }

    // if this is the last reduce task took over, should wait until the reactive tasks are done.
    // on completion of reactive task, we will receive an extra message from job tracker.
    
    if ( stopContext != null ) {
        // first we need to wait on stopContext
        LOG.info("wait for pending scanning task");
        // FIXME for local scan vs. parallel scan 
        final int MAX_WAIT_TIME = job.getBoolean(SkewTuneJobConfig.SKEWTUNE_REPARTITION_LOCALSCAN_FORCE,false) ? 20 : 3;
        int waitCount = 0;
        while ( waitCount++ < MAX_WAIT_TIME && ! stopContext.awaitTermination() ) {
            LOG.info("still waiting for pending scanning task: "+waitCount);
        }
        
        if ( waitCount >= MAX_WAIT_TIME ) {
            throw new IllegalStateException("scan task is running way longer than expected!");
        } else {
            LOG.info("scan task has completed");
        }
    }
    
    if ( splitted.get() ) {
        LOG.info("wait for completion of reactive takeover job");
        while ( ! canceled.get() ) {
            try {
                synchronized (canceled) {
                    canceled.wait(10000);
                }
            } catch ( InterruptedException ignore ) {
                LOG.info("interrupted while waiting... silently ignore.");
            }
            reporter.progress(); // I'm alive!!
        }
        
        LOG.info("received cancel message due to completion of reactive takeover job");
    }
    
    done(umbilical, reporter);
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runOldReducer(final JobConf job,
                     TaskUmbilicalProtocol umbilical,
                     final TaskReporter reporter,
                     RawKeyValueIterator rIter,
                     final RawComparator<INKEY> comparator,
                     final Class<INKEY> keyClass,
                     Class<INVALUE> valueClass) throws IOException {
      
    Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer = 
      ReflectionUtils.newInstance(job.getReducerClass(), job);
    // make output collector
    String finalName = getOutputName(getPartition());

    FileSystem fs = FileSystem.get(job);

    final RecordWriter<OUTKEY,OUTVALUE> out = 
      job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);  
    
    OutputCollector<OUTKEY,OUTVALUE> collector = 
      new OutputCollector<OUTKEY,OUTVALUE>() {
        public void collect(OUTKEY key, OUTVALUE value)
          throws IOException {
          out.write(key, value);
          reduceOutputCounter.increment(1);
          // indicate that progress update needs to be sent
          reporter.progress();
        }
      };

    // apply reduce function
    try {
      //increment processed counter only if skipping feature is enabled
      boolean incrProcCount = SkipBadRecords.getReducerMaxSkipGroups(job)>0 &&
        SkipBadRecords.getAutoIncrReducerProcCount(job);
      
      ReduceValuesIterator<INKEY,INVALUE> values = isSkipping() ? 
          new SkippingReduceValuesIterator<INKEY,INVALUE>(rIter, 
              comparator, keyClass, valueClass, 
              job, reporter, umbilical) :
          new ReduceValuesIterator<INKEY,INVALUE>(rIter, 
          job.getOutputValueGroupingComparator(), keyClass, valueClass, 
          job, reporter);
      values.informReduceProgress();

      final ReduceValuesIterator<INKEY,INVALUE> wrapValues = ( reportSkewReduce ) ? new SplittableReduceValuesIterator<INKEY,INVALUE>(values) : values;

      if ( reportSkewReduce ) {
//          this.splitRemainAction = new SplitRemainingDataAction() {
//            @Override
//            public void splitRemaining(int n, boolean cont, DataOutputBuffer buf)
//                    throws IOException {
//                SerializationFactory factory = new SerializationFactory(job);
//                Serializer<INKEY> serializer = factory.getSerializer(keyClass);
//                serializer.open(buf);
//                ((SplittableReduceValuesIterator<INKEY,INVALUE>)wrapValues).split(n, serializer); // will mark current key and stop
//                serializer.close();
//            }
//          };
          
          TellAndStopAction action = new TellAndStopAction() {
            @Override
            public Future<StopStatus> tellAndStop(StopContext context) throws IOException {
                SerializationFactory factory = new SerializationFactory(job);
                Serializer<INKEY> serializer = factory.getSerializer(keyClass);
                serializer.open(context.getBuffer());
                ((SplittableReduceValuesIterator<INKEY,INVALUE>)wrapValues).tellAndStop(serializer);
                serializer.close();
                return StopStatus.future(StopStatus.STOPPED);
            }
            };
            
            if ( ! setupStopHandler(action) ) {
                LOG.warn("this task has been handovered!");
                throw new InterruptedIOException();
            }
      }
      
      if ( myProgress != null ) {
          myProgress.endSetup();
          myProgress.beginCompute();
      }
      
      while (wrapValues.more()) {
        reduceInputKeyCounter.increment(1);
        reducer.reduce(wrapValues.getKey(), wrapValues, collector, reporter);
        if(incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
              SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS, 1);
        }
        if ( myProgress != null ) {
            ((TaskProgress.ReduceProgress)myProgress).beginNewReduce(); // flush buffered things
        }
        wrapValues.nextKey();
        wrapValues.informReduceProgress();
      }
      
      //Clean up: repeated in catch block below
      reducer.close();
      out.close(reporter);
            
      //End of clean up.
    } catch (IOException ioe) {
      try {
        reducer.close();
      } catch (IOException ignored) {}
        
      try {
        out.close(reporter);
      } catch (IOException ignored) {}
      
      throw ioe;
    }
  }

  static class NewTrackingRecordWriter<K,V> 
      extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
    private final org.apache.hadoop.mapreduce.RecordWriter<K,V> real;
    private final org.apache.hadoop.mapreduce.Counter outputRecordCounter;
  
    NewTrackingRecordWriter(org.apache.hadoop.mapreduce.RecordWriter<K,V> real,
                            org.apache.hadoop.mapreduce.Counter recordCounter) {
      this.real = real;
      this.outputRecordCounter = recordCounter;
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
    InterruptedException {
      real.close(context);
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      real.write(key,value);
      outputRecordCounter.increment(1);
    }
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runNewReducer(JobConf job,
                     final TaskUmbilicalProtocol umbilical,
                     final TaskReporter reporter,
                     RawKeyValueIterator rIter,
                     RawComparator<INKEY> comparator,
                     Class<INKEY> keyClass,
                     Class<INVALUE> valueClass
                     ) throws IOException,InterruptedException, 
                              ClassNotFoundException {
      if ( myProgress != null ) {
          myProgress.beginSetup();
      }
      
    // wrap value iterator to report progress.
    final RawKeyValueIterator rawIter = rIter;
    rIter = new RawKeyValueIterator() {
      public void close() throws IOException {
        rawIter.close();
      }
      public DataInputBuffer getKey() throws IOException {
        return rawIter.getKey();
      }
      public Progress getProgress() {
        return rawIter.getProgress();
      }
      public DataInputBuffer getValue() throws IOException {
        return rawIter.getValue();
      }
      public boolean next() throws IOException {
        boolean ret = rawIter.next();
        reporter.setProgress(rawIter.getProgress().getProgress());
        return ret;
      }
    };
    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
      new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job, getTaskID());
    // make a reducer
    org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer =
      (org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>)
        ReflectionUtils.newInstance(taskContext.getReducerClass(), job);
    org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> output =
      (org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE>)
        outputFormat.getRecordWriter(taskContext);
    org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> trackedRW = 
      new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(output, reduceOutputCounter);
    job.setBoolean("mapred.skip.on", isSkipping());
    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());
    org.apache.hadoop.mapreduce.Reducer.Context 
         reducerContext = createReduceContext(reducer, job, getTaskID(),
                                               rIter, reduceInputKeyCounter, 
                                               reduceInputValueCounter, 
                                               trackedRW,
                                               committer,
                                               reporter, comparator, keyClass,
                                               valueClass);
    
    reducer.run(reducerContext);
    output.close(reducerContext);
  }
  
  
  

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runSkewTuneOldReducer(final JobConf job,
                     TaskUmbilicalProtocol umbilical,
                     final TaskReporter reporter,
                     RawKeyValueIterator rIter,
                     final RawComparator<INKEY> comparator,
                     final Class<INKEY> keyClass,
                     Class<INVALUE> valueClass) throws IOException, InterruptedException {
      
    Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer = 
      ReflectionUtils.newInstance(job.getReducerClass(), job);
    // make output collector
    
    // check whether we have the schedule info. if so, override the default name
    int[] order = getScheduleOrder();
    String finalName = getOutputName( order == null ? getPartition() : order[getPartition()]);

    FileSystem fs = FileSystem.get(job);

    final RecordWriter<OUTKEY,OUTVALUE> out = 
      job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);  
    
    OutputCollector<OUTKEY,OUTVALUE> collector = 
      new OutputCollector<OUTKEY,OUTVALUE>() {
        public void collect(OUTKEY key, OUTVALUE value)
          throws IOException {
          out.write(key, value);
          reduceOutputCounter.increment(1);
          // indicate that progress update needs to be sent
          reporter.progress();
        }
      };
      
    final OldReduceInputContext<INKEY,INVALUE> context = new OldReduceInputContext<INKEY,INVALUE>(
            job,
            getTaskID(),
            rIter,
            reduceInputKeyCounter,
            reduceInputValueCounter,
            reporter,
            comparator,
            keyClass,
            valueClass,
            (TaskProgress.ReduceProgress)myProgress);

    // apply reduce function
    try {
        TellAndStopAction action = new TellAndStopAction() {
            @Override
            public Future<StopStatus> tellAndStop(StopContext stopContext) throws IOException, InterruptedException {
                return context.stop(stopContext);
            }
            };
            
            if ( ! setupStopHandler(action) ) {
                LOG.warn("this task has been handovered!");
                throw new InterruptedIOException();
            }
        
      if ( myProgress != null ) {
          myProgress.endSetup();
          myProgress.beginCompute();
      }
      
      while ( context.nextKey() ) {
          INKEY key = context.getCurrentKey();
          reducer.reduce(key, context.getValues().iterator(), collector, reporter);
          if ( myProgress != null ) {
            ((TaskProgress.ReduceProgress)myProgress).beginNewReduce(); // flush buffered things
          }
          reducePhase.set(rIter.getProgress().getProgress()); // update progress
          reporter.progress();
      }
      
      LOG.info("reduce completed. reduce input values="+reduceInputValueCounter.getCounter());
      
      //Clean up: repeated in catch block below
      reducer.close();
      out.close(reporter);
            
      //End of clean up.
    } catch (IOException ioe) {
      try {
        reducer.close();
      } catch (IOException ignored) {}
        
      try {
        out.close(reporter);
      } catch (IOException ignored) {}
      
      throw ioe;
    }
  }
  
  class SortStopAction implements TellAndStopAction, Future<StopStatus> {
      final Thread computeThread;
      AtomicBoolean interrupted = new AtomicBoolean();
      volatile StopContext stopContext;
      StopStatus result;
      
      SortStopAction() {
          computeThread = Thread.currentThread();
      }

      SortStopAction(StopContext ctx) {
          computeThread = Thread.currentThread();
          this.stopContext = ctx;
      }
      
    @Override
    public synchronized Future<StopStatus> tellAndStop(StopContext context)
            throws IOException, InterruptedException {
        this.stopContext = context;
        interrupted.set(true);
        TaskStatus.Phase phase = getPhase();
        if ( phase != TaskStatus.Phase.REDUCE ) {
            if ( phase == TaskStatus.Phase.SHUFFLE ) {
                context.emptyReduceResponse();
                result = StopStatus.STOPPED;
            }
            computeThread.interrupt();
        }
        return this;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public synchronized StopStatus get() throws InterruptedException, ExecutionException {
        if ( result == null ) {
            wait();
        }
        return result;
    }

    @Override
    public synchronized StopStatus get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if ( result == null ) {
            if ( unit == TimeUnit.SECONDS ) timeout *= 1000;
            wait(timeout);
        }
        return result;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public synchronized boolean isDone() {
        return result != null;
    }
    
    public synchronized void setStopWithEmptyResponse() throws IOException {
        stopContext.emptyReduceResponse();
        result = StopStatus.STOPPED;
        notifyAll();
    }
    
    public synchronized void setStatus(StopStatus s) {
        result = s;
        notifyAll();
    }
    
    public boolean isInterrupted() { return interrupted.get(); }
    
    public void scan(JobConf job,RawKeyValueIterator rIter,TaskReporter reporter,RawComparator comparator,Class keyClass,Class valueClass) {
        StopStatus s = StopStatus.CANNOT_STOP;
        
        try {
            final OldReduceInputContext context = new OldReduceInputContext(
                    job,
                    getTaskID(),
                    rIter,
                    reduceInputKeyCounter,
                    reduceInputValueCounter,
                    reporter,
                    comparator,
                    keyClass,
                    valueClass,
                    (TaskProgress.ReduceProgress)myProgress);
            
            // FIXME double check whether the remaining bytes is less than X

            s = context.scan(stopContext);
        } catch (Exception ex) {
            LOG.error(ex);
        }
        
        setStatus(s);
        
        LOG.info("Scan completion state = "+result);
    }
  }
}
