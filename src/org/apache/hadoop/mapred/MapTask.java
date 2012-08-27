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
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.JobSplit.SplitMetaInfo;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.sun.org.apache.xml.internal.security.utils.Base64;

import skewtune.mapreduce.SkewTuneJobConfig;
import skewtune.mapreduce.protocol.SRTaskStatus;
import skewtune.mapreduce.protocol.SkewTuneTaskUmbilicalProtocol;

/** A Map task. */
class MapTask extends Task {
    /**
     * The size of each record in the index file for the map-outputs.
     */
    public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;

    private TaskSplitIndex splitMetaInfo = new TaskSplitIndex();

    private String splitClass;

    private final static int APPROX_HEADER_LENGTH = 150;

    private static final Log LOG = LogFactory.getLog(MapTask.class.getName());

    private Progress mapPhase;

    private Progress sortPhase;

    { // set phase for this task
        setPhase(TaskStatus.Phase.MAP);
        getProgress().setStatus("map");
    }

    // SKEWREDUCE
    private boolean forceSort;
    private boolean allWrittenAlready;
    private short defaultRep;
    private long defaultBlockSize;
    private Path defaultWorkFile;
    private Path defaultIndexFile;
    private final long SPILL_PADDING = 4*1024*1024; // 4MB padding to final spill file
    
    public MapTask() {
        super();
    }

    public MapTask(String jobFile, TaskAttemptID taskId, int partition,
            TaskSplitIndex splitIndex, int numSlotsRequired) {
        super(jobFile, taskId, partition, numSlotsRequired);
        this.splitMetaInfo = splitIndex;
    }

    @Override
    public boolean isMapTask() {
        return true;
    }

    @Override
    public void localizeConfiguration(JobConf conf) throws IOException {
        super.localizeConfiguration(conf);
        // split.dta/split.info files are used only by IsolationRunner.
        // Write the split file to the local disk if it is a normal map task
        // (not a
        // job-setup or a job-cleanup task) and if the user wishes to run
        // IsolationRunner either by setting keep.failed.tasks.files to true or
        // by
        // using keep.tasks.files.pattern
        if (supportIsolationRunner(conf) && isMapOrReduce()) {
            // localize the split meta-information
            Path localSplitMeta = new LocalDirAllocator(MRConfig.LOCAL_DIR)
                    .getLocalPathForWrite(TaskTracker.getLocalSplitMetaFile(
                            conf.getUser(), getJobID().toString(), getTaskID()
                                    .toString()), conf);
            LOG.debug("Writing local split to " + localSplitMeta);
            DataOutputStream out = FileSystem.getLocal(conf).create(
                    localSplitMeta);
            splitMetaInfo.write(out);
            out.close();
        }
    }

    @Override
    public TaskRunner createRunner(TaskTracker tracker,
            TaskTracker.TaskInProgress tip) {
        return new MapTaskRunner(tip, tracker, this.conf);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if (isMapOrReduce()) {
            splitMetaInfo.write(out);
            splitMetaInfo = null;
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if (isMapOrReduce()) {
            splitMetaInfo.readFields(in);
        }
    }

    /**
     * This class wraps the user's record reader to update the counters and
     * progress as records are read.
     * 
     * @param <K>
     * @param <V>
     */
    class TrackedRecordReader<K, V> implements SplittableRecordReader<K, V> {
        private RecordReader<K, V> rawIn;

        private Counters.Counter inputByteCounter;
        private Counters.Counter inputRecordCounter;
        private Counters.Counter procByteCounter;
        private Counters.Counter procRecordCounter;

        private TaskReporter reporter;

        private long beforePos = -1;
        private long afterPos = -1;
        private long processingBytes;
        
        private boolean stopped;

        TrackedRecordReader(RecordReader<K, V> raw, TaskReporter reporter)
                throws IOException {
            rawIn = raw;
            inputRecordCounter = reporter
                    .getCounter(TaskCounter.MAP_INPUT_RECORDS);
            inputByteCounter = reporter.getCounter(
                    FileInputFormat.COUNTER_GROUP, FileInputFormat.BYTES_READ);
            procByteCounter = reporter.getCounter("SkewReduce","PROC_BYTES");
            procRecordCounter = reporter.getCounter("SkewReduce","PROC_RECORDS");
            this.reporter = reporter;
        }

        public K createKey() {
            return rawIn.createKey();
        }

        public V createValue() {
            return rawIn.createValue();
        }

        public synchronized boolean next(K key, V value) throws IOException {
            if ( stopped ) return false;
            
            if ( myProgress != null ) {
                ((TaskProgress.MapProgress)myProgress).beginNewMap();
            }
            
            boolean ret = moveToNext(key, value);
            procByteCounter.increment(processingBytes);
            if ( processingBytes != 0 ) {
                procRecordCounter.increment(1);
            }
            
            if (ret) {
                incrCounters();
            } else {
                processingBytes = 0;
            }
            return ret;
        }

        protected void incrCounters() {
            inputRecordCounter.increment(1);
            processingBytes = afterPos - beforePos;
            inputByteCounter.increment(processingBytes);
        }

        protected synchronized boolean moveToNext(K key, V value)
                throws IOException {
            beforePos = getPos();
            boolean ret = rawIn.next(key, value);
            afterPos = getPos();
            reporter.setProgress(getProgress());
            return ret;
        }

        public long getPos() throws IOException {
            return rawIn.getPos();
        }

        public void close() throws IOException {
            rawIn.close();
        }

        public float getProgress() throws IOException {
            return rawIn.getProgress();
        }

        TaskReporter getTaskReporter() {
            return reporter;
        }
/*
        @Override
        public List<InputSplit> split(int n, boolean cont) throws IOException {
            if ( rawIn instanceof SplittableRecordReader ) {
                return ((SplittableRecordReader<K,V>)rawIn).split(n,cont);
            }
            return Collections.emptyList();
        }
*/
        @Override
        public StopStatus tellAndStop(DataOutput out) throws IOException {
            if ( rawIn instanceof SplittableRecordReader ) {
                return ((SplittableRecordReader<K,V>)rawIn).tellAndStop(out);
            }
            return StopStatus.CANNOT_STOP;
        }
    }

    /**
     * This class skips the records based on the failed ranges from previous
     * attempts.
     */
    class SkippingRecordReader<K, V> extends TrackedRecordReader<K, V> {
        private SkipRangeIterator skipIt;

        private SequenceFile.Writer skipWriter;

        private boolean toWriteSkipRecs;

        private TaskUmbilicalProtocol umbilical;

        private Counters.Counter skipRecCounter;

        private long recIndex = -1;

        SkippingRecordReader(RecordReader<K, V> raw,
                TaskUmbilicalProtocol umbilical, TaskReporter reporter)
                throws IOException {
            super(raw, reporter);
            this.umbilical = umbilical;
            this.skipRecCounter = reporter
                    .getCounter(TaskCounter.MAP_SKIPPED_RECORDS);
            this.toWriteSkipRecs = toWriteSkipRecs()
                    && SkipBadRecords.getSkipOutputPath(conf) != null;
            skipIt = getSkipRanges().skipRangeIterator();
        }

        public synchronized boolean next(K key, V value) throws IOException {
            if (!skipIt.hasNext()) {
                LOG.warn("Further records got skipped.");
                return false;
            }
            boolean ret = moveToNext(key, value);
            long nextRecIndex = skipIt.next();
            long skip = 0;
            while (recIndex < nextRecIndex && ret) {
                if (toWriteSkipRecs) {
                    writeSkippedRec(key, value);
                }
                ret = moveToNext(key, value);
                skip++;
            }
            // close the skip writer once all the ranges are skipped
            if (skip > 0 && skipIt.skippedAllRanges() && skipWriter != null) {
                skipWriter.close();
            }
            skipRecCounter.increment(skip);
            reportNextRecordRange(umbilical, recIndex);
            if (ret) {
                incrCounters();
            }
            return ret;
        }

        protected synchronized boolean moveToNext(K key, V value)
                throws IOException {
            recIndex++;
            return super.moveToNext(key, value);
        }

        @SuppressWarnings("unchecked")
        private void writeSkippedRec(K key, V value) throws IOException {
            if (skipWriter == null) {
                Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
                Path skipFile = new Path(skipDir, getTaskID().toString());
                skipWriter = SequenceFile.createWriter(
                        skipFile.getFileSystem(conf), conf, skipFile,
                        (Class<K>) createKey().getClass(),
                        (Class<V>) createValue().getClass(),
                        CompressionType.BLOCK, getTaskReporter());
            }
            skipWriter.append(key, value);
        }
    }

    @Override
    public void run(final JobConf job, final TaskUmbilicalProtocol umbilical,
            final SkewTuneTaskUmbilicalProtocol srumbilical)
            throws IOException, ClassNotFoundException, InterruptedException {
        this.umbilical = umbilical;
        this.srumbilical = srumbilical;

        if (isMapTask()) {
            mapPhase = getProgress().addPhase("map", 0.667f);
            sortPhase = getProgress().addPhase("sort", 0.333f);
        }
        myProgress = new TaskProgress.MapProgress(job, getCounters(), taskStatus, mapPhase, sortPhase);
        TaskReporter reporter = startReporter(umbilical, srumbilical, myProgress);

        boolean useNewApi = job.getUseNewMapper();
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
        if (job.getBoolean(SkewTuneJobConfig.ENABLE_SKEWTUNE_MAP, false) && srumbilical != null ) {
            // okay, time to enable monitoring
            try {
                srumbilical.init(getTaskID(), job.getNumMapTasks(),job.getNumReduceTasks());
                this.srTaskStatus = new SRTaskStatus(this.getTaskID());
//                srTaskStatus.setStartTime(System.currentTimeMillis());
//                myProgress = new TaskProgress.MapProgress(job, counters, taskStatus, mapPhase, sortPhase);
                reportSkewReduce = true;
                LOG.info("enabling skewreduce monitoring");
                SkewNotifier.setBaseUrl(job,getTaskID());
                SkewNotifier.startNotifier(); // we allow start it multiple times.
            } catch (IOException e) {
                LOG.warn("unable to contact skewreduce tracker", e);
            }
        }
        
        if ( myProgress != null ) {
            myProgress.beginTask();
        }

        if (useNewApi) {
            runNewMapper(job, splitMetaInfo, umbilical, reporter);
        } else if ( reportSkewReduce ) {
            runSkewTuneOldMapper(job, splitMetaInfo, umbilical, reporter);
        } else {
            runOldMapper(job, splitMetaInfo, umbilical, reporter);
        }
        
        if ( myProgress != null ) {
            myProgress.endTask();
        }
        
        done(umbilical, reporter);
    }

    @SuppressWarnings("unchecked")
    private <T> T getSplitDetails(Path file, long offset) throws IOException {
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream inFile = fs.open(file);
        inFile.seek(offset);
        String className = Text.readString(inFile);
        Class<T> cls;
        try {
            cls = (Class<T>) conf.getClassByName(className);
        } catch (ClassNotFoundException ce) {
            IOException wrap = new IOException("Split class " + className
                    + " not found");
            wrap.initCause(ce);
            throw wrap;
        }
        SerializationFactory factory = new SerializationFactory(conf);
        Deserializer<T> deserializer = (Deserializer<T>) factory
                .getDeserializer(cls);
        deserializer.open(inFile);
        T split = deserializer.deserialize(null);
        long pos = inFile.getPos();
        getCounters().findCounter(TaskCounter.SPLIT_RAW_BYTES).increment(
                pos - offset);
        inFile.close();
        return split;
    }

    @SuppressWarnings("unchecked")
    private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runOldMapper(
            final JobConf job, final TaskSplitIndex splitIndex,
            final TaskUmbilicalProtocol umbilical, TaskReporter reporter)
            throws IOException, InterruptedException, ClassNotFoundException {
//        InputSplit inputSplit = getSplitDetails(
//                new Path(splitIndex.getSplitLocation()),
//                splitIndex.getStartOffset());
        
        if ( myProgress != null ) {
            myProgress.beginSetup();
        }
        
        InputSplit inputSplit = getInputSplit(splitIndex);

        updateJobWithSplit(job, inputSplit);
        reporter.setInputSplit(inputSplit);

        RecordReader<INKEY, INVALUE> rawIn = // open input
        job.getInputFormat().getRecordReader(inputSplit, job, reporter);
        final RecordReader<INKEY, INVALUE> in = isSkipping() ? new SkippingRecordReader<INKEY, INVALUE>(
                rawIn, umbilical, reporter)
                : new TrackedRecordReader<INKEY, INVALUE>(rawIn, reporter);
        job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());

        int numReduceTasks = conf.getNumReduceTasks();
        LOG.info("numReduceTasks: " + numReduceTasks);
        MapOutputCollector collector = null;
        
        // FIXME check whether we need to build spilt action
//        if ( rawIn instanceof SplittableRecordReader ) {
//            TellAndStopAction action = new TellAndStopAction() {
//                @Override
//                public Future<StopStatus> tellAndStop(StopContext context)
//                        throws IOException {
//                    return new StopStatus.Future(((SplittableRecordReader)in).tellAndStop(out));
//                }
//            };
//            
//            setupStopHandler(action);
//        }

        forceSort = job.getBoolean(SkewTuneJobConfig.MAP_OUTPUT_SORT_ATTR,false);
        if (forceSort) {
            defaultRep = (short) job.getInt(
                    SkewTuneJobConfig.MAP_OUTPUT_REPLICATION_ATTR,
                    job.getInt("dfs.replication", 1));
            defaultWorkFile = FileOutputFormat.getTaskOutputPath(job, getOutputName(getPartition()));
            defaultIndexFile = new Path( defaultWorkFile.getParent(), "." + defaultWorkFile.getName() + ".index");

            LOG.debug("default replication level = " + defaultRep);
            LOG.info("setting default output to " + defaultWorkFile);
            LOG.info("setting default index to " + defaultIndexFile);
        }

        if (forceSort || numReduceTasks > 0) {
            collector = new MapOutputBuffer(umbilical, job, reporter);
        } else {
            collector = new DirectMapOutputCollector(umbilical, job, reporter);
        }
        MapRunnable<INKEY, INVALUE, OUTKEY, OUTVALUE> runner = ReflectionUtils
                .newInstance(job.getMapRunnerClass(), job);

        try {
            OutputCollector<OUTKEY,OUTVALUE> oldCollector = new OldOutputCollector(collector,conf);
            
            if ( myProgress != null ) {
                myProgress.endSetup();
                myProgress.beginCompute();
            }
            
            runner.run(in, oldCollector, reporter);
            mapPhase.complete();

            if ( myProgress != null ) {
                myProgress.beginSort();
            }
            setPhase(TaskStatus.Phase.SORT);
            statusUpdate(umbilical);
            collector.flush();
        } finally {
            // close
            in.close(); // close input
            collector.close();
        }
    }

    /**
     * Update the job with details about the file split
     * 
     * @param job
     *            the job configuration to update
     * @param inputSplit
     *            the file split
     */
    private void updateJobWithSplit(final JobConf job, InputSplit inputSplit) {
        if (inputSplit instanceof FileSplit) {
            FileSplit fileSplit = (FileSplit) inputSplit;
            job.set(JobContext.MAP_INPUT_FILE, fileSplit.getPath().toString());
            job.setLong(JobContext.MAP_INPUT_START, fileSplit.getStart());
            job.setLong(JobContext.MAP_INPUT_PATH, fileSplit.getLength());
        }
    }

    static class NewTrackingRecordReader<K, V> extends
            org.apache.hadoop.mapreduce.RecordReader<K, V> {
        private final org.apache.hadoop.mapreduce.RecordReader<K, V> real;
        private final org.apache.hadoop.mapreduce.Counter inputRecordCounter;
        private final org.apache.hadoop.mapreduce.Counter procRecordCounter;
        private boolean hasProcessed;

        private final TaskReporter reporter;

        NewTrackingRecordReader(
                org.apache.hadoop.mapreduce.RecordReader<K, V> real,
                TaskReporter reporter) {
            this.real = real;
            this.reporter = reporter;
            this.inputRecordCounter = reporter.getCounter(TaskCounter.MAP_INPUT_RECORDS);
            this.procRecordCounter = reporter.getCounter("SkewReduce","PROC_RECORDS");
        }

        @Override
        public void close() throws IOException {
            real.close();
        }

        @Override
        public K getCurrentKey() throws IOException, InterruptedException {
            return real.getCurrentKey();
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return real.getCurrentValue();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return real.getProgress();
        }

        @Override
        public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
                org.apache.hadoop.mapreduce.TaskAttemptContext context)
                throws IOException, InterruptedException {
            real.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if ( hasProcessed ) {
                procRecordCounter.increment(1);
                hasProcessed = false;
            }
            boolean result = real.nextKeyValue();
            if (result) {
                inputRecordCounter.increment(1);
                hasProcessed = true;
            }
            reporter.setProgress(getProgress());
            return result;
        }
    }

    /**
     * Since the mapred and mapreduce Partitioners don't share a common
     * interface (JobConfigurable is deprecated and a subtype of
     * mapred.Partitioner), the partitioner lives in Old/NewOutputCollector.
     * Note that, for map-only jobs, the configured partitioner should not be
     * called. It's common for partitioners to compute a result mod numReduces,
     * which causes a div0 error
     */
    private class OldOutputCollector<K, V> implements OutputCollector<K, V> {
        private final Partitioner<K, V> partitioner;

        private final MapOutputCollector<K, V> collector;

        private final int numPartitions;

        @SuppressWarnings("unchecked")
        OldOutputCollector(MapOutputCollector<K, V> collector, JobConf conf) {
            int numReduces = conf.getNumReduceTasks();
//            numPartitions = conf.getNumReduceTasks();
            if ( numReduces > 1) {
                numPartitions = numReduces;
                partitioner = (Partitioner<K, V>) ReflectionUtils.newInstance(
                        conf.getPartitionerClass(), conf);
            } else if (forceSort) {
                // do we have a partitioner class?
                String partitionerClass = jobContext.getConfiguration().get(SkewTuneJobConfig.ORIGINAL_PARTITIONER_CLASS_ATTR);
                final int orgReducers = jobContext.getConfiguration().getInt(SkewTuneJobConfig.ORIGINAL_NUM_REDUCES, 1);
                numPartitions = orgReducers;
                
                if ( partitionerClass == null ) {
                    // the application did not explicitly specified partitioner class. we fall back to hash partitioner
                    partitionerClass = HashPartitioner.class.getName();
                }
                
                try {
                    partitioner = (Partitioner<K, V>) ReflectionUtils
                            .newInstance(jobContext.getConfiguration()
                                    .getClassByName(partitionerClass), conf);
                } catch (ClassNotFoundException e) {
                    LOG.fatal("Unable to load partitioner class "
                            + partitionerClass);
                    throw new IllegalArgumentException(e);
                }
            } else {
                numPartitions = numReduces;
                partitioner = new Partitioner<K, V>() {
                    @Override
                    public void configure(JobConf job) {
                    }

                    @Override
                    public int getPartition(K key, V value, int numPartitions) {
                        return numPartitions - 1;
                    }
                };
            }
            this.collector = collector;
        }

        @Override
        public void collect(K key, V value) throws IOException {
            try {
                collector.collect(key, value,
                        partitioner.getPartition(key, value, numPartitions));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupt exception", ie);
            }
        }
    }

    private class NewDirectOutputCollector<K, V> extends
            org.apache.hadoop.mapreduce.RecordWriter<K, V> {
        private final org.apache.hadoop.mapreduce.RecordWriter out;

        private final TaskReporter reporter;

        private final Counters.Counter mapOutputRecordCounter;

        @SuppressWarnings("unchecked")
        NewDirectOutputCollector(MRJobConfig jobContext, JobConf job,
                TaskUmbilicalProtocol umbilical, TaskReporter reporter)
                throws IOException, ClassNotFoundException,
                InterruptedException {
            this.reporter = reporter;
            out = outputFormat.getRecordWriter(taskContext);
            mapOutputRecordCounter = reporter
                    .getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void write(K key, V value) throws IOException,
                InterruptedException {
            reporter.progress();
            out.write(key, value);
            mapOutputRecordCounter.increment(1);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
                InterruptedException {
            reporter.progress();
            if (out != null) {
                out.close(context);
            }
        }
    }

    private class NewOutputCollector<K, V> extends
            org.apache.hadoop.mapreduce.RecordWriter<K, V> {
        private final MapOutputCollector<K, V> collector;

        private final org.apache.hadoop.mapreduce.Partitioner<K, V> partitioner;

        private final int partitions;

        @SuppressWarnings("unchecked")
        NewOutputCollector(org.apache.hadoop.mapreduce.JobContext jobContext,
                JobConf job, TaskUmbilicalProtocol umbilical,
                TaskReporter reporter) throws IOException,
                ClassNotFoundException {
            collector = new MapOutputBuffer<K, V>(umbilical, job, reporter);
//            partitions = jobContext.getNumReduceTasks();
            int numReduces = jobContext.getNumReduceTasks();
            if (numReduces > 1) {
                partitions = numReduces;
                partitioner = (org.apache.hadoop.mapreduce.Partitioner<K, V>) ReflectionUtils
                        .newInstance(jobContext.getPartitionerClass(), job);
            } else if (forceSort) {
                // do we have a partitioner class?
                String partitionerClass = jobContext.getConfiguration().get(
                        SkewTuneJobConfig.ORIGINAL_PARTITIONER_CLASS_ATTR);
                final int orgReducers = jobContext.getConfiguration().getInt(SkewTuneJobConfig.ORIGINAL_NUM_REDUCES, 1);
                partitions = orgReducers;

                if (partitionerClass == null) {
                    // should be the default
                    // the partitioner was not explicitly specified. we assume it was a hash partitioner
                    partitionerClass = org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class.getName();
                }
                partitioner = (org.apache.hadoop.mapreduce.Partitioner<K, V>) ReflectionUtils
                        .newInstance(jobContext.getConfiguration()
                                .getClassByName(partitionerClass), job);
            } else {
                partitions = numReduces;
                partitioner = new org.apache.hadoop.mapreduce.Partitioner<K, V>() {
                    @Override
                    public int getPartition(K key, V value, int numPartitions) {
                        return partitions - 1;
                    }
                };
            }
        }

        @Override
        public void write(K key, V value) throws IOException,
                InterruptedException {
            collector.collect(key, value,
                    partitioner.getPartition(key, value, partitions));
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
                InterruptedException {
            try {
                collector.flush();
            } catch (ClassNotFoundException cnf) {
                throw new IOException("can't find class ", cnf);
            }
            collector.close();
        }
    }

    @SuppressWarnings("unchecked")
    private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runNewMapper(
            final JobConf job, final TaskSplitIndex splitIndex,
            final TaskUmbilicalProtocol umbilical, TaskReporter reporter)
            throws IOException, ClassNotFoundException, InterruptedException {
        
        if ( myProgress != null ) {
            myProgress.beginSetup();
        }
        
        // make a task context so we can get the classes
        org.apache.hadoop.mapreduce.TaskAttemptContext taskContext = new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(
                job, getTaskID());
        // make a mapper
        org.apache.hadoop.mapreduce.Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper = (org.apache.hadoop.mapreduce.Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>) ReflectionUtils
                .newInstance(taskContext.getMapperClass(), job);
        // make the input format
        org.apache.hadoop.mapreduce.InputFormat<INKEY, INVALUE> inputFormat = (org.apache.hadoop.mapreduce.InputFormat<INKEY, INVALUE>) ReflectionUtils
                .newInstance(taskContext.getInputFormatClass(), job);
        // rebuild the input split
        org.apache.hadoop.mapreduce.InputSplit split = null;
        split = getSplitDetails(new Path(splitIndex.getSplitLocation()),
                splitIndex.getStartOffset());

        org.apache.hadoop.mapreduce.RecordReader<INKEY, INVALUE> input = new NewTrackingRecordReader<INKEY, INVALUE>(
                inputFormat.createRecordReader(split, taskContext), reporter);

        job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());
        org.apache.hadoop.mapreduce.RecordWriter output = null;

        forceSort = job.getBoolean(SkewTuneJobConfig.MAP_OUTPUT_SORT_ATTR,false);
        if (forceSort) {
            defaultRep = (short) job.getInt(
                    SkewTuneJobConfig.MAP_OUTPUT_REPLICATION_ATTR,
                    job.getInt("dfs.replication", 1));
            if (LOG.isDebugEnabled()) {
                LOG.debug("default replication level = " + defaultRep);
            }
        }

        // get an output object
        if (forceSort || job.getNumReduceTasks() > 0) {
            output = new NewOutputCollector(taskContext, job, umbilical,
                    reporter);
        } else {
            output = new NewDirectOutputCollector(taskContext, job, umbilical,
                    reporter);
        }

        org.apache.hadoop.mapreduce.MapContext<INKEY, INVALUE, OUTKEY, OUTVALUE> mapContext = new MapContextImpl<INKEY, INVALUE, OUTKEY, OUTVALUE>(
                job, getTaskID(), input, output, committer, reporter, split);

        org.apache.hadoop.mapreduce.Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context mapperContext = new WrappedMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>()
                .getMapContext(mapContext);

        if (forceSort) {
            if (outputFormat instanceof org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
                defaultWorkFile = ((org.apache.hadoop.mapreduce.lib.output.FileOutputFormat<?, ?>) outputFormat)
                        .getDefaultWorkFile(mapContext, "");
            } else {
                LOG.warn("can't identify temporary file. fall back to default output path");
                defaultWorkFile = new Path(
                        FileOutputFormat.getWorkOutputPath(job),
                        getOutputName(getPartition()));
            }
            defaultIndexFile = new Path(defaultWorkFile.getParent(),
                    defaultWorkFile.getName() + ".index");
            LOG.info("setting default output to " + defaultWorkFile);
            LOG.info("setting default index to " + defaultIndexFile);
        }

        input.initialize(split, mapperContext);
        
        mapper.run(mapperContext);
        mapPhase.complete();
        
        if ( myProgress != null ) {
            myProgress.beginSort();
        }
        
        // sort begin
        setPhase(TaskStatus.Phase.SORT);
        statusUpdate(umbilical);
        input.close();
        output.close(mapperContext);
    }

    interface MapOutputCollector<K, V> {

        public void collect(K key, V value, int partition) throws IOException,
                InterruptedException;

        public void close() throws IOException, InterruptedException;

        public void flush() throws IOException, InterruptedException,
                ClassNotFoundException;

    }

    class DirectMapOutputCollector<K, V> implements MapOutputCollector<K, V> {

        private RecordWriter<K, V> out = null;

        private TaskReporter reporter = null;

        private final Counters.Counter mapOutputRecordCounter;

        @SuppressWarnings("unchecked")
        public DirectMapOutputCollector(TaskUmbilicalProtocol umbilical,
                JobConf job, TaskReporter reporter) throws IOException {
            this.reporter = reporter;
            int[] order = getScheduleOrder();
            String finalName = getOutputName( order == null ? getPartition() : order[getPartition()]);
//            String finalName = getOutputName(getPartition());
            FileSystem fs = FileSystem.get(job);

            out = job.getOutputFormat().getRecordWriter(fs, job, finalName,
                    reporter);

            mapOutputRecordCounter = reporter
                    .getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
        }

        public void close() throws IOException {
            if (this.out != null) {
                out.close(this.reporter);
            }

        }

        public void flush() throws IOException, InterruptedException,
                ClassNotFoundException {
        }

        public void collect(K key, V value, int partition) throws IOException {
            reporter.progress();
            out.write(key, value);
            mapOutputRecordCounter.increment(1);
        }

    }

    private class MapOutputBuffer<K extends Object, V extends Object>
            implements MapOutputCollector<K, V>, IndexedSortable {
        final int partitions;

        final JobConf job;

        final TaskReporter reporter;

        final Class<K> keyClass;

        final Class<V> valClass;

        final RawComparator<K> comparator;

        final SerializationFactory serializationFactory;

        final Serializer<K> keySerializer;

        final Serializer<V> valSerializer;

        final CombinerRunner<K, V> combinerRunner;

        final CombineOutputCollector<K, V> combineCollector;

        // Compression for map-outputs
        final CompressionCodec codec;

        // k/v accounting
        final IntBuffer kvmeta; // metadata overlay on backing store

        int kvstart; // marks origin of spill metadata

        int kvend; // marks end of spill metadata

        int kvindex; // marks end of fully serialized records

        int equator; // marks origin of meta/serialization

        int bufstart; // marks beginning of spill

        int bufend; // marks beginning of collectable

        int bufmark; // marks end of record

        int bufindex; // marks end of collected

        int bufvoid; // marks the point where we should stop
                     // reading at the end of the buffer

        byte[] kvbuffer; // main output buffer

        private final byte[] b0 = new byte[0];

        private static final int INDEX = 0; // index offset in acct

        private static final int VALSTART = 1; // val offset in acct

        private static final int KEYSTART = 2; // key offset in acct

        private static final int PARTITION = 3; // partition offset in acct

        private static final int NMETA = 4; // num meta ints

        private static final int METASIZE = NMETA * 4; // size in bytes

        // spill accounting
        final int maxRec;

        final int softLimit;

        boolean spillInProgress;;

        int bufferRemaining;

        volatile Throwable sortSpillException = null;

        int numSpills = 0;

        final int minSpillsForCombine;

        final IndexedSorter sorter;

        final ReentrantLock spillLock = new ReentrantLock();

        final Condition spillDone = spillLock.newCondition();

        final Condition spillReady = spillLock.newCondition();

        final BlockingBuffer bb = new BlockingBuffer();

        volatile boolean spillThreadRunning = false;

        final SpillThread spillThread = new SpillThread();

        final FileSystem rfs;

        // Counters
        final Counters.Counter mapOutputByteCounter;

        final Counters.Counter mapOutputRecordCounter;

        final ArrayList<SpillRecord> indexCacheList = new ArrayList<SpillRecord>();

        private int totalIndexCacheMemory;

        private static final int INDEX_CACHE_MEMORY_LIMIT = 1024 * 1024;

        @SuppressWarnings("unchecked")
        public MapOutputBuffer(TaskUmbilicalProtocol umbilical, JobConf job,
                TaskReporter reporter) throws IOException,
                ClassNotFoundException {
            this.job = job;
            this.reporter = reporter;
            partitions = forceSort ? job.getInt(
                    SkewTuneJobConfig.ORIGINAL_NUM_REDUCES, 1) : job
                    .getNumReduceTasks();
            rfs = ((LocalFileSystem) FileSystem.getLocal(job)).getRaw();

            // sanity checks
            final float spillper = job.getFloat(
                    JobContext.MAP_SORT_SPILL_PERCENT, (float) 0.8);
            final int sortmb = job.getInt(JobContext.IO_SORT_MB, 100);
            if (spillper > (float) 1.0 || spillper <= (float) 0.0) {
                throw new IOException("Invalid \""
                        + JobContext.MAP_SORT_SPILL_PERCENT + "\": " + spillper);
            }
            if ((sortmb & 0x7FF) != sortmb) {
                throw new IOException("Invalid \"" + JobContext.IO_SORT_MB
                        + "\": " + sortmb);
            }
            sorter = ReflectionUtils.newInstance(job.getClass("map.sort.class",
                    QuickSort.class, IndexedSorter.class), job);
            // buffers and accounting
            int maxMemUsage = sortmb << 20;
            maxMemUsage -= maxMemUsage % METASIZE;
            kvbuffer = new byte[maxMemUsage];
            bufvoid = kvbuffer.length;
            kvmeta = ByteBuffer.wrap(kvbuffer).asIntBuffer();
            setEquator(0);
            bufstart = bufend = bufindex = equator;
            kvstart = kvend = kvindex;

            maxRec = kvmeta.capacity() / NMETA;
            softLimit = (int) (kvbuffer.length * spillper);
            bufferRemaining = softLimit;
            if (LOG.isInfoEnabled()) {
                LOG.info(JobContext.IO_SORT_MB + ": " + sortmb);
                LOG.info("soft limit at " + softLimit);
                LOG.info("bufstart = " + bufstart + "; bufvoid = " + bufvoid);
                LOG.info("kvstart = " + kvstart + "; length = " + maxRec);
            }

            // k/v serialization
            comparator = job.getOutputKeyComparator();
            keyClass = (Class<K>) job.getMapOutputKeyClass();
            valClass = (Class<V>) job.getMapOutputValueClass();
            serializationFactory = new SerializationFactory(job);
            keySerializer = serializationFactory.getSerializer(keyClass);
            keySerializer.open(bb);
            valSerializer = serializationFactory.getSerializer(valClass);
            valSerializer.open(bb);

            // output counters
            mapOutputByteCounter = reporter.getCounter(TaskCounter.MAP_OUTPUT_BYTES);
            mapOutputRecordCounter = reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS);

            // compression
            if (job.getCompressMapOutput()) {
                Class<? extends CompressionCodec> codecClass = job
                        .getMapOutputCompressorClass(DefaultCodec.class);
                codec = ReflectionUtils.newInstance(codecClass, job);
            } else {
                codec = null;
            }

            // combiner
            final Counters.Counter combineInputCounter = reporter
                    .getCounter(TaskCounter.COMBINE_INPUT_RECORDS);
            combinerRunner = CombinerRunner.create(job, getTaskID(),
                    combineInputCounter, reporter, null);
            if (combinerRunner != null) {
                final Counters.Counter combineOutputCounter = reporter
                        .getCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
                combineCollector = new CombineOutputCollector<K, V>(combineOutputCounter);
            } else {
                combineCollector = null;
            }
            spillInProgress = false;
            minSpillsForCombine = job.getInt(JobContext.MAP_COMBINE_MIN_SPILLS, 3);
            spillThread.setDaemon(true);
            spillThread.setName("SpillThread");
            spillLock.lock();
            try {
                spillThread.start();
                while (!spillThreadRunning) {
                    spillDone.await();
                }
            } catch (InterruptedException e) {
                throw new IOException("Spill thread failed to initialize", e);
            } finally {
                spillLock.unlock();
            }
            if (sortSpillException != null) {
                throw new IOException("Spill thread failed to initialize",
                        sortSpillException);
            }
        }

        /**
         * Serialize the key, value to intermediate storage. When this method
         * returns, kvindex must refer to sufficient unused storage to store one
         * METADATA.
         */
        public synchronized void collect(K key, V value, final int partition)
                throws IOException {
            reporter.progress();
            if (key.getClass() != keyClass) {
                throw new IOException(
                        "Type mismatch in key from map: expected "
                                + keyClass.getName() + ", recieved "
                                + key.getClass().getName());
            }
            if (value.getClass() != valClass) {
                throw new IOException(
                        "Type mismatch in value from map: expected "
                                + valClass.getName() + ", recieved "
                                + value.getClass().getName());
            }
            if (partition < 0 || partition >= partitions) {
                throw new IOException("Illegal partition for " + key + " ("
                        + partition + ")");
            }
            checkSpillException();
            bufferRemaining -= METASIZE;
            if (bufferRemaining <= 0) {
                // start spill if the thread is not running and the soft limit
                // has been
                // reached
                spillLock.lock();
                try {
                    do {
                        if (!spillInProgress) {
                            final int kvbidx = 4 * kvindex;
                            final int kvbend = 4 * kvend;
                            // serialized, unspilled bytes always lie between
                            // kvindex and
                            // bufindex, crossing the equator. Note that any
                            // void space
                            // created by a reset must be included in "used"
                            // bytes
                            final int bUsed = distanceTo(kvbidx, bufindex);
                            final boolean bufsoftlimit = bUsed >= softLimit;
                            if ((kvbend + METASIZE) % kvbuffer.length != equator
                                    - (equator % METASIZE)) {
                                // spill finished, reclaim space
                                resetSpill();
                                bufferRemaining = Math.min(
                                        distanceTo(bufindex, kvbidx) - 2
                                                * METASIZE, softLimit - bUsed)
                                        - METASIZE;
                                continue;
                            } else if (bufsoftlimit && kvindex != kvend) {
                                // spill records, if any collected; check
                                // latter, as it may
                                // be possible for metadata alignment to hit
                                // spill pcnt
                                startSpill();
                                final int avgRec = (int) (mapOutputByteCounter
                                        .getCounter() / mapOutputRecordCounter
                                        .getCounter());
                                // leave at least half the split buffer for
                                // serialization data
                                // ensure that kvindex >= bufindex
                                final int distkvi = distanceTo(bufindex, kvbidx);
                                final int newPos = (bufindex + Math.max(
                                        2 * METASIZE - 1,
                                        Math.min(distkvi / 2, distkvi
                                                / (METASIZE + avgRec)
                                                * METASIZE)))
                                        % kvbuffer.length;
                                setEquator(newPos);
                                bufmark = bufindex = newPos;
                                final int serBound = 4 * kvend;
                                // bytes remaining before the lock must be held
                                // and limits
                                // checked is the minimum of three arcs: the
                                // metadata space, the
                                // serialization space, and the soft limit
                                bufferRemaining = Math.min(
                                // metadata max
                                        distanceTo(bufend, newPos), Math.min(
                                        // serialization max
                                                distanceTo(newPos, serBound),
                                                // soft limit
                                                softLimit)) - 2 * METASIZE;
                            }
                        }
                    } while (false);
                } finally {
                    spillLock.unlock();
                }
            }

            try {
                // serialize key bytes into buffer
                int keystart = bufindex;
                keySerializer.serialize(key);
                if (bufindex < keystart) {
                    // wrapped the key; must make contiguous
                    bb.shiftBufferedKey();
                    keystart = 0;
                }
                // serialize value bytes into buffer
                final int valstart = bufindex;
                valSerializer.serialize(value);
                // It's possible for records to have zero length, i.e. the
                // serializer
                // will perform no writes. To ensure that the boundary
                // conditions are
                // checked and that the kvindex invariant is maintained, perform
                // a
                // zero-length write into the buffer. The logic monitoring this
                // could be
                // moved into collect, but this is cleaner and inexpensive. For
                // now, it
                // is acceptable.
                bb.write(b0, 0, 0);

                // the record must be marked after the preceding write, as the
                // metadata
                // for this record are not yet written
                int valend = bb.markRecord();

                mapOutputRecordCounter.increment(1);
                mapOutputByteCounter.increment(distanceTo(keystart, valend,
                        bufvoid));

                // write accounting info
                kvmeta.put(kvindex + INDEX, kvindex);
                kvmeta.put(kvindex + PARTITION, partition);
                kvmeta.put(kvindex + KEYSTART, keystart);
                kvmeta.put(kvindex + VALSTART, valstart);
                // advance kvindex
                kvindex = (kvindex - NMETA + kvmeta.capacity())
                        % kvmeta.capacity();
            } catch (MapBufferTooSmallException e) {
                LOG.info("Record too large for in-memory buffer: "
                        + e.getMessage());
                spillSingleRecord(key, value, partition);
                mapOutputRecordCounter.increment(1);
                return;
            }
        }

        /**
         * Set the point from which meta and serialization data expand. The meta
         * indices are aligned with the buffer, so metadata never spans the ends
         * of the circular buffer.
         */
        private void setEquator(int pos) {
            equator = pos;
            // set index prior to first entry, aligned at meta boundary
            final int aligned = pos - (pos % METASIZE);
            kvindex = ((aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
            if (LOG.isInfoEnabled()) {
                LOG.info("(EQUATOR) " + pos + " kvi " + kvindex + "("
                        + (kvindex * 4) + ")");
            }
        }

        /**
         * The spill is complete, so set the buffer and meta indices to be equal
         * to the new equator to free space for continuing collection. Note that
         * when kvindex == kvend == kvstart, the buffer is empty.
         */
        private void resetSpill() {
            final int e = equator;
            bufstart = bufend = e;
            final int aligned = e - (e % METASIZE);
            // set start/end to point to first meta record
            kvstart = kvend = ((aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
            if (LOG.isInfoEnabled()) {
                LOG.info("(RESET) equator " + e + " kv " + kvstart + "("
                        + (kvstart * 4) + ")" + " kvi " + kvindex + "("
                        + (kvindex * 4) + ")");
            }
        }

        /**
         * Compute the distance in bytes between two indices in the
         * serialization buffer.
         * 
         * @see #distanceTo(int,int,int)
         */
        final int distanceTo(final int i, final int j) {
            return distanceTo(i, j, kvbuffer.length);
        }

        /**
         * Compute the distance between two indices in the circular buffer given
         * the max distance.
         */
        int distanceTo(final int i, final int j, final int mod) {
            return i <= j ? j - i : mod - i + j;
        }

        /**
         * For the given meta position, return the dereferenced position in the
         * integer array. Each meta block contains several integers describing
         * record data in its serialized form, but the INDEX is not necessarily
         * related to the proximate metadata. The index value at the referenced
         * int position is the start offset of the associated metadata block. So
         * the metadata INDEX at metapos may point to the metadata described by
         * the metadata block at metapos + k, which contains information about
         * that serialized record.
         */
        int offsetFor(int metapos) {
            return kvmeta.get(metapos * NMETA + INDEX);
        }

        /**
         * Compare logical range, st i, j MOD offset capacity. Compare by
         * partition, then by key.
         * 
         * @see IndexedSortable#compare
         */
        public int compare(final int mi, final int mj) {
            final int kvi = offsetFor(mi % maxRec);
            final int kvj = offsetFor(mj % maxRec);
            final int kvip = kvmeta.get(kvi + PARTITION);
            final int kvjp = kvmeta.get(kvj + PARTITION);
            // sort by partition
            if (kvip != kvjp) {
                return kvip - kvjp;
            }
            // sort by key
            return comparator.compare(kvbuffer, kvmeta.get(kvi + KEYSTART),
                    kvmeta.get(kvi + VALSTART) - kvmeta.get(kvi + KEYSTART),
                    kvbuffer, kvmeta.get(kvj + KEYSTART),
                    kvmeta.get(kvj + VALSTART) - kvmeta.get(kvj + KEYSTART));
        }

        /**
         * Swap logical indices st i, j MOD offset capacity.
         * 
         * @see IndexedSortable#swap
         */
        public void swap(final int mi, final int mj) {
            final int kvi = (mi % maxRec) * NMETA + INDEX;
            final int kvj = (mj % maxRec) * NMETA + INDEX;
            int tmp = kvmeta.get(kvi);
            kvmeta.put(kvi, kvmeta.get(kvj));
            kvmeta.put(kvj, tmp);
        }

        /**
         * Inner class managing the spill of serialized records to disk.
         */
        protected class BlockingBuffer extends DataOutputStream {

            public BlockingBuffer() {
                super(new Buffer());
            }

            /**
             * Mark end of record. Note that this is required if the buffer is
             * to cut the spill in the proper place.
             */
            public int markRecord() {
                bufmark = bufindex;
                return bufindex;
            }

            /**
             * Set position from last mark to end of writable buffer, then
             * rewrite the data between last mark and kvindex. This handles a
             * special case where the key wraps around the buffer. If the key is
             * to be passed to a RawComparator, then it must be contiguous in
             * the buffer. This recopies the data in the buffer back into
             * itself, but starting at the beginning of the buffer. Note that
             * this method should <b>only</b> be called immediately after
             * detecting this condition. To call it at any other time is
             * undefined and would likely result in data loss or corruption.
             * 
             * @see #markRecord()
             */
            protected void shiftBufferedKey() throws IOException {
                // spillLock unnecessary; both kvend and kvindex are current
                int headbytelen = bufvoid - bufmark;
                bufvoid = bufmark;
                final int kvbidx = 4 * kvindex;
                final int kvbend = 4 * kvend;
                final int avail = Math.min(distanceTo(0, kvbidx),
                        distanceTo(0, kvbend));
                if (bufindex + headbytelen < avail) {
                    System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen,
                            bufindex);
                    System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0,
                            headbytelen);
                    bufindex += headbytelen;
                    bufferRemaining -= kvbuffer.length - bufvoid;
                } else {
                    byte[] keytmp = new byte[bufindex];
                    System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
                    bufindex = 0;
                    out.write(kvbuffer, bufmark, headbytelen);
                    out.write(keytmp);
                }
            }
        }

        public class Buffer extends OutputStream {
            private final byte[] scratch = new byte[1];

            @Override
            public void write(int v) throws IOException {
                scratch[0] = (byte) v;
                write(scratch, 0, 1);
            }

            /**
             * Attempt to write a sequence of bytes to the collection buffer.
             * This method will block if the spill thread is running and it
             * cannot write.
             * 
             * @throws MapBufferTooSmallException
             *             if record is too large to deserialize into the
             *             collection buffer.
             */
            @Override
            public void write(byte b[], int off, int len) throws IOException {
                // must always verify the invariant that at least METASIZE bytes
                // are
                // available beyond kvindex, even when len == 0
                bufferRemaining -= len;
                if (bufferRemaining <= 0) {
                    // writing these bytes could exhaust available buffer space
                    // or fill
                    // the buffer to soft limit. check if spill or blocking are
                    // necessary
                    boolean blockwrite = false;
                    spillLock.lock();
                    try {
                        do {
                            checkSpillException();

                            final int kvbidx = 4 * kvindex;
                            final int kvbend = 4 * kvend;
                            // ser distance to key index
                            final int distkvi = distanceTo(bufindex, kvbidx);
                            // ser distance to spill end index
                            final int distkve = distanceTo(bufindex, kvbend);

                            // if kvindex is closer than kvend, then a spill is
                            // neither in
                            // progress nor complete and reset since the lock
                            // was held. The
                            // write should block only if there is insufficient
                            // space to
                            // complete the current write, write the metadata
                            // for this record,
                            // and write the metadata for the next record. If
                            // kvend is closer,
                            // then the write should block if there is too
                            // little space for
                            // either the metadata or the current write. Note
                            // that collect
                            // ensures its metadata requirement with a
                            // zero-length write
                            blockwrite = distkvi <= distkve ? distkvi <= len
                                    + 2 * METASIZE
                                    : distkve <= len
                                            || distanceTo(bufend, kvbidx) < 2 * METASIZE;

                            if (!spillInProgress) {
                                if (blockwrite) {
                                    if ((kvbend + METASIZE) % kvbuffer.length != equator
                                            - (equator % METASIZE)) {
                                        // spill finished, reclaim space
                                        // need to use meta exclusively;
                                        // zero-len rec & 100% spill
                                        // pcnt would fail
                                        resetSpill(); // resetSpill doesn't move
                                                      // bufindex, kvindex
                                        bufferRemaining = Math.min(distkvi - 2
                                                * METASIZE, softLimit
                                                - distanceTo(kvbidx, bufindex))
                                                - len;
                                        continue;
                                    }
                                    // we have records we can spill; only spill
                                    // if blocked
                                    if (kvindex != kvend) {
                                        startSpill();
                                        // Blocked on this write, waiting for
                                        // the spill just
                                        // initiated to finish. Instead of
                                        // repositioning the marker
                                        // and copying the partial record, we
                                        // set the record start
                                        // to be the new equator
                                        setEquator(bufmark);
                                    } else {
                                        // We have no buffered records, and this
                                        // record is too large
                                        // to write into kvbuffer. We must spill
                                        // it directly from
                                        // collect
                                        final int size = distanceTo(bufstart,
                                                bufindex) + len;
                                        setEquator(0);
                                        bufstart = bufend = bufindex = equator;
                                        kvstart = kvend = kvindex;
                                        bufvoid = kvbuffer.length;
                                        throw new MapBufferTooSmallException(
                                                size + " bytes");
                                    }
                                }
                            }

                            if (blockwrite) {
                                // wait for spill
                                try {
                                    while (spillInProgress) {
                                        reporter.progress();
                                        spillDone.await();
                                    }
                                } catch (InterruptedException e) {
                                    throw new IOException(
                                            "Buffer interrupted while waiting for the writer",
                                            e);
                                }
                            }
                        } while (blockwrite);
                    } finally {
                        spillLock.unlock();
                    }
                }
                // here, we know that we have sufficient space to write
                if (bufindex + len > bufvoid) {
                    final int gaplen = bufvoid - bufindex;
                    System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
                    len -= gaplen;
                    off += gaplen;
                    bufindex = 0;
                }
                System.arraycopy(b, off, kvbuffer, bufindex, len);
                bufindex += len;
            }
        }

        public void flush() throws IOException, ClassNotFoundException,
                InterruptedException {
            LOG.info("Starting flush of map output");
            spillLock.lock();
            try {
                while (spillInProgress) {
                    reporter.progress();
                    spillDone.await();
                }
                checkSpillException();

                final int kvbend = 4 * kvend;
                if ((kvbend + METASIZE) % kvbuffer.length != equator
                        - (equator % METASIZE)) {
                    // spill finished
                    resetSpill();
                }
                if (kvindex != kvend) {
                    kvend = (kvindex + NMETA) % kvmeta.capacity();
                    bufend = bufmark;
                    if (LOG.isInfoEnabled()) {
                        LOG.info("Spilling map output");
                        LOG.info("bufstart = " + bufstart + "; bufend = "
                                + bufmark + "; bufvoid = " + bufvoid);
                        LOG.info("kvstart = "
                                + kvstart
                                + "("
                                + (kvstart * 4)
                                + "); kvend = "
                                + kvend
                                + "("
                                + (kvend * 4)
                                + "); length = "
                                + (distanceTo(kvend, kvstart, kvmeta.capacity()) + 1)
                                + "/" + maxRec);
                    }
                    // note that this is the last call. if there was no spill,
                    // the output should go into DFS
                    sortAndSpill(true);
                }
            } catch (InterruptedException e) {
                throw new IOException(
                        "Interrupted while waiting for the writer", e);
            } finally {
                spillLock.unlock();
            }
            assert !spillLock.isHeldByCurrentThread();
            // shut down spill thread and wait for it to exit. Since the
            // preceding
            // ensures that it is finished with its work (and sortAndSpill did
            // not
            // throw), we elect to use an interrupt instead of setting a flag.
            // Spilling simultaneously from this thread while the spill thread
            // finishes its work might be both a useful way to extend this and
            // also
            // sufficient motivation for the latter approach.
            try {
                spillThread.interrupt();
                spillThread.join();
            } catch (InterruptedException e) {
                throw new IOException("Spill failed", e);
            }
            // release sort buffer before the merge
            kvbuffer = null;
            mergeParts();
        }

        public void close() {
        }

        protected class SpillThread extends Thread {

            @Override
            public void run() {
                spillLock.lock();
                spillThreadRunning = true;
                try {
                    while (true) {
                        spillDone.signal();
                        while (!spillInProgress) {
                            spillReady.await();
                        }
                        try {
                            spillLock.unlock();
                            sortAndSpill(false);
                        } catch (Throwable t) {
                            sortSpillException = t;
                        } finally {
                            spillLock.lock();
                            if (bufend < bufstart) {
                                bufvoid = kvbuffer.length;
                            }
                            kvstart = kvend;
                            bufstart = bufend;
                            spillInProgress = false;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    spillLock.unlock();
                    spillThreadRunning = false;
                }
            }
        }

        private void checkSpillException() throws IOException {
            final Throwable lspillException = sortSpillException;
            if (lspillException != null) {
                if (lspillException instanceof Error) {
                    final String logMsg = "Task " + getTaskID() + " failed : "
                            + StringUtils.stringifyException(lspillException);
                    reportFatalError(getTaskID(), lspillException, logMsg);
                }
                throw new IOException("Spill failed", lspillException);
            }
        }

        private void startSpill() {
            assert !spillInProgress;
            kvend = (kvindex + NMETA) % kvmeta.capacity();
            bufend = bufmark;
            spillInProgress = true;
            if (LOG.isInfoEnabled()) {
                LOG.info("Spilling map output");
                LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark
                        + "; bufvoid = " + bufvoid);
                LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4)
                        + "); kvend = " + kvend + "(" + (kvend * 4)
                        + "); length = "
                        + (distanceTo(kvend, kvstart, kvmeta.capacity()) + 1)
                        + "/" + maxRec);
            }
            spillReady.signal();
        }

        private void sortAndSpill(boolean lastCall) throws IOException,
                ClassNotFoundException, InterruptedException {
            // approximate the length of the output file to be the length of the
            // buffer + header lengths for the partitions
            long inputBytes = (bufend >= bufstart ? bufend - bufstart : (bufvoid - bufend) + bufstart);
            final long size = inputBytes
                    + partitions
                    * APPROX_HEADER_LENGTH;
            
            FSDataOutputStream out = null;
            boolean writeToHDFS = forceSort && lastCall && numSpills == 0;
            try {
                // create spill file
                final SpillRecord spillRec = new SpillRecord(partitions);
                final MapOutputIndex mapOutIndex = writeToHDFS ? new MapOutputIndex(partitions) : null;

                if (writeToHDFS) {
                    // SKEWREDUCE
                    String finalName = getOutputName(getPartition());
                    FileSystem fs = FileSystem.get(job);
                    int bufSize = job.getInt("io.file.buffer.size", 4096);
                    int checksumSize = job.getInt("io.bytes.per.checksum", 512);
                    defaultBlockSize = (long) (Math.ceil(((size + SPILL_PADDING) / checksumSize)) * checksumSize);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("default block size = " + defaultBlockSize);
                    }
                    
                    // path, permission, overwrite, buffersize, replication,
                    // blocksize, progress
                    out = fs.create(defaultWorkFile, new FsPermission(
                            (short) 0700), true, bufSize, defaultRep,
                            defaultBlockSize, reporter);
                } else {
                    final Path filename = mapOutputFile.getSpillFileForWrite(numSpills, size);
                    out = rfs.create(filename);
                }

                final int mstart = kvend / NMETA;
                final int mend = 1 + // kvend is a valid record
                        (kvstart >= kvend ? kvstart : kvmeta.capacity()
                                + kvstart) / NMETA;
                sorter.sort(MapOutputBuffer.this, mstart, mend, reporter);
                
                long startTime = System.currentTimeMillis();
                long inputRecords = 0;
                long outputBytes = 0;
                long outputRecords = combineCollector == null ? 0 : combineCollector.getCounter();
                
                int spindex = mstart;
                final IndexRecord rec = new IndexRecord();
                final InMemValBytes value = new InMemValBytes();
                for (int i = 0; i < partitions; ++i) {
                    IFile.Writer<K, V> writer = null;
                    try {
                        long segmentStart = out.getPos();
                        writer = new Writer<K, V>(job, out, keyClass, valClass, codec, spilledRecordsCounter);
                        if (combinerRunner == null) {
                            // spill directly
                            DataInputBuffer key = new DataInputBuffer();
                            int spstart = spindex;
                            while (spindex < mend
                                    && kvmeta.get(offsetFor(spindex % maxRec)
                                            + PARTITION) == i) {
                                final int kvoff = offsetFor(spindex % maxRec);
                                key.reset(kvbuffer, kvmeta
                                        .get(kvoff + KEYSTART), (kvmeta
                                        .get(kvoff + VALSTART) - kvmeta
                                        .get(kvoff + KEYSTART)));
                                getVBytesForOffset(kvoff, value);
                                writer.append(key, value);
                                ++spindex;
                            }
                            inputRecords += (spindex - spstart);
                            outputRecords += (spindex - spstart);
                        } else {
                            int spstart = spindex;
                            while (spindex < mend
                                    && kvmeta.get(offsetFor(spindex % maxRec) + PARTITION) == i) {
                                ++spindex;
                            }
                            // Note: we would like to avoid the combiner if we've fewer
                            // than some threshold of records for a partition
                            if (spstart != spindex) {
                                combineCollector.setWriter(writer);
                                RawKeyValueIterator kvIter = new MRResultIterator(spstart, spindex);
                                combinerRunner.combine(kvIter, combineCollector);
                            }
                            inputRecords += (spindex - spstart);
                        }

                        // close the writer
                        writer.close();

                        // record offsets
                        rec.startOffset = segmentStart;
                        rec.rawLength = writer.getRawLength();
                        rec.partLength = writer.getCompressedLength();
                        
                        outputBytes += rec.rawLength;

                        if (writeToHDFS) {
                            mapOutIndex.putIndex(rec, i);
                        } else {
                            spillRec.putIndex(rec, i);
                        }

                        writer = null;
                    } finally {
                        if (null != writer)
                            writer.close();
                    }
                }
                
                if ( combineCollector != null ) {
                    outputRecords = combineCollector.getCounter() - outputRecords;
                }
                
                long endTime = System.currentTimeMillis();
                
                if ( myProgress != null ) {
                    ((TaskProgress.MapProgress)myProgress).addSpill(endTime - startTime, inputBytes, inputRecords, outputBytes, outputRecords);
                }

                if (totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
                    // create spill index file
                    Path indexFilename = mapOutputFile
                            .getSpillIndexFileForWrite(numSpills, partitions
                                    * MAP_OUTPUT_INDEX_RECORD_LENGTH);
                    spillRec.writeToFile(indexFilename, job);
                } else {
                    // if forceSort and this is the first and last spill, we
                    // always fall here.
                    indexCacheList.add(spillRec);
                    totalIndexCacheMemory += spillRec.size()
                            * MAP_OUTPUT_INDEX_RECORD_LENGTH;
                    if (writeToHDFS) {
                        if ( srTaskStatus != null ) {
                            if ( LOG.isDebugEnabled() ) {
                                LOG.debug("setting map output index for the single spill : "+mapOutIndex);
                            }
                            srTaskStatus.setMapOutputIndex(mapOutIndex);
                        }
                        mapOutIndex.writeToFile(defaultIndexFile, job,
                                defaultRep);
                        allWrittenAlready = true;
                    }
                }
                LOG.info("Finished spill " + numSpills);
                ++numSpills;
            } finally {
                if (out != null)
                    out.close();
            }
        }

        /**
         * Handles the degenerate case where serialization fails to fit in the
         * in-memory buffer, so we must spill the record from collect directly
         * to a spill file. Consider this "losing".
         */
        private void spillSingleRecord(final K key, final V value, int partition)
                throws IOException {
            long size = kvbuffer.length + partitions * APPROX_HEADER_LENGTH;
            FSDataOutputStream out = null;
            try {
                // create spill file
                final SpillRecord spillRec = new SpillRecord(partitions);
                final Path filename = mapOutputFile.getSpillFileForWrite(
                        numSpills, size);
                out = rfs.create(filename);

                // we don't run the combiner for a single record
                IndexRecord rec = new IndexRecord();
                for (int i = 0; i < partitions; ++i) {
                    IFile.Writer<K, V> writer = null;
                    try {
                        long segmentStart = out.getPos();
                        // Create a new codec, don't care!
                        writer = new IFile.Writer<K, V>(job, out, keyClass,
                                valClass, codec, spilledRecordsCounter);

                        if (i == partition) {
                            final long recordStart = out.getPos();
                            writer.append(key, value);
                            // Note that our map byte count will not be accurate
                            // with
                            // compression
                            mapOutputByteCounter.increment(out.getPos()
                                    - recordStart);
                        }
                        writer.close();

                        // record offsets
                        rec.startOffset = segmentStart;
                        rec.rawLength = writer.getRawLength();
                        rec.partLength = writer.getCompressedLength();
                        spillRec.putIndex(rec, i);

                        writer = null;
                    } catch (IOException e) {
                        if (null != writer)
                            writer.close();
                        throw e;
                    }
                }
                if (totalIndexCacheMemory >= INDEX_CACHE_MEMORY_LIMIT) {
                    // create spill index file
                    Path indexFilename = mapOutputFile
                            .getSpillIndexFileForWrite(numSpills, partitions
                                    * MAP_OUTPUT_INDEX_RECORD_LENGTH);
                    spillRec.writeToFile(indexFilename, job);
                } else {
                    indexCacheList.add(spillRec);
                    totalIndexCacheMemory += spillRec.size()
                            * MAP_OUTPUT_INDEX_RECORD_LENGTH;
                }
                ++numSpills;
            } finally {
                if (out != null)
                    out.close();
            }
        }

        /**
         * Given an offset, populate vbytes with the associated set of
         * deserialized value bytes. Should only be called during a spill.
         */
        private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
            // get the keystart for the next serialized value to be the end
            // of this value. If this is the last value in the buffer, use
            // bufend
            final int nextindex = kvoff == kvend ? bufend : kvmeta.get((kvoff
                    - NMETA + kvmeta.capacity() + KEYSTART)
                    % kvmeta.capacity());
            // calculate the length of the value
            int vallen = (nextindex >= kvmeta.get(kvoff + VALSTART)) ? nextindex
                    - kvmeta.get(kvoff + VALSTART)
                    : (bufvoid - kvmeta.get(kvoff + VALSTART)) + nextindex;
            vbytes.reset(kvbuffer, kvmeta.get(kvoff + VALSTART), vallen);
        }

        /**
         * Inner class wrapping valuebytes, used for appendRaw.
         */
        protected class InMemValBytes extends DataInputBuffer {
            private byte[] buffer;

            private int start;

            private int length;

            public void reset(byte[] buffer, int start, int length) {
                this.buffer = buffer;
                this.start = start;
                this.length = length;

                if (start + length > bufvoid) {
                    this.buffer = new byte[this.length];
                    final int taillen = bufvoid - start;
                    System.arraycopy(buffer, start, this.buffer, 0, taillen);
                    System.arraycopy(buffer, 0, this.buffer, taillen, length
                            - taillen);
                    this.start = 0;
                }

                super.reset(this.buffer, this.start, this.length);
            }
        }

        protected class MRResultIterator implements RawKeyValueIterator {
            private final DataInputBuffer keybuf = new DataInputBuffer();

            private final InMemValBytes vbytes = new InMemValBytes();

            private final int end;

            private int current;

            public MRResultIterator(int start, int end) {
                this.end = end;
                current = start - 1;
            }

            public boolean next() throws IOException {
                return ++current < end;
            }

            public DataInputBuffer getKey() throws IOException {
                final int kvoff = offsetFor(current % maxRec);
                keybuf.reset(
                        kvbuffer,
                        kvmeta.get(kvoff + KEYSTART),
                        kvmeta.get(kvoff + VALSTART)
                                - kvmeta.get(kvoff + KEYSTART));
                return keybuf;
            }

            public DataInputBuffer getValue() throws IOException {
                getVBytesForOffset(offsetFor(current % maxRec), vbytes);
                return vbytes;
            }

            public Progress getProgress() {
                return null;
            }

            public void close() {
            }
        }

        class WrappedSpillRecord {
            final SpillRecord sr;

            final MapOutputIndex mapOutIndex;

            WrappedSpillRecord(int partitions) {
                if (forceSort) {
                    mapOutIndex = new MapOutputIndex(partitions);
                    sr = null;
                } else {
                    mapOutIndex = null;
                    sr = new SpillRecord(partitions);
                }
            }

            public void putIndex(IndexRecord rec, int partition) {
                if (sr == null) {
                    mapOutIndex.putIndex(rec, partition);
                } else {
                    sr.putIndex(rec, partition);
                }
            }

            public void writeToFile(Path loc, JobConf job) throws IOException {
                if (sr == null) {
                    if ( srTaskStatus != null ) {
                        if ( LOG.isDebugEnabled() ) {
                            LOG.debug("setting map output index for multiple spills : "+mapOutIndex);
                        }
                        srTaskStatus.setMapOutputIndex(mapOutIndex);
                    }
                    mapOutIndex.writeToFile(loc, job, defaultRep);
                } else {
                    sr.writeToFile(loc, job);
                }
            }
        }

        private void mergeParts() throws IOException, InterruptedException,
                ClassNotFoundException {
            // get the approximate size of the final output/index files
            long finalOutFileSize = 0;
            long finalIndexFileSize = 0;
            final Path[] filename = new Path[numSpills];
            final TaskAttemptID mapId = getTaskID();

            if (allWrittenAlready) {
                return; // we are all done! only happens when there was exactly
                        // one spill by flush
            }

            for (int i = 0; i < numSpills; i++) {
                filename[i] = mapOutputFile.getSpillFile(i);
                finalOutFileSize += rfs.getFileStatus(filename[i]).getLen();
            }

            if (numSpills == 1) { // the spill is the final output
                if (forceSort) {
                    // if this is the case, it must be the degenerated case caused by a single record too large to fit in memory.
                    // first, copy local data file to HDFS
                    FileSystem fs = FileSystem.get(job);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("copying " + filename[0] + " to " + defaultWorkFile);
                    }
                    // FIXME should properly setup the replication factor and block size
                    fs.copyFromLocalFile(filename[0], defaultWorkFile);

                    // then, copy local index file to HDFS
                    MapOutputIndex mapOutIndex = null;
                    if (indexCacheList.size() == 0) {
                        Path localIndexFile = mapOutputFile.getSpillIndexFile(0);
                        mapOutIndex = new MapOutputIndex(localIndexFile, job,true);
                    } else {
                        mapOutIndex = new MapOutputIndex(indexCacheList.get(0));
                    }
                    
                    if ( srTaskStatus != null ) {
                        if ( LOG.isDebugEnabled() ) {
                            LOG.debug("setting map output index for degenerated single spill : "+mapOutIndex);
                        }
                        srTaskStatus.setMapOutputIndex(mapOutIndex);
                    }
                    mapOutIndex.writeToFile(defaultIndexFile, job, defaultRep);
                } else {
                    rfs.rename(filename[0], new Path(filename[0].getParent(),"file.out"));
                    if (indexCacheList.size() == 0) {
                        rfs.rename(mapOutputFile.getSpillIndexFile(0),
                                new Path(filename[0].getParent(),"file.out.index"));
                    } else {
                        indexCacheList.get(0).writeToFile(
                                new Path(filename[0].getParent(),"file.out.index"), job);
                    }
                }
                return;
            }

            // read in paged indices
            for (int i = indexCacheList.size(); i < numSpills; ++i) {
                Path indexFileName = mapOutputFile.getSpillIndexFile(i);
                indexCacheList.add(new SpillRecord(indexFileName, job));
            }

            // make correction in the length to include the sequence file header
            // lengths for each partition
            finalOutFileSize += partitions * APPROX_HEADER_LENGTH;
            finalIndexFileSize = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;
            Path finalOutputFile = (forceSort) ? defaultWorkFile : mapOutputFile
                    .getOutputFileForWrite(finalOutFileSize);
            Path finalIndexFile = (forceSort) ? defaultIndexFile : mapOutputFile
                    .getOutputIndexFileForWrite(finalIndexFileSize);

            // The output stream for the final single output file
            FSDataOutputStream finalOut = null;

            // SKEWREDUCE
            if (forceSort) {
                // if there was more than one spill, we should create the final
                // output
                FileSystem fs = FileSystem.get(job);
                int bufSize = job.getInt("io.file.buffer.size", 4096);
                int checksumSize = job.getInt("io.bytes.per.checksum", 512);
                FsPermission perm = new FsPermission((short) 0700);
                defaultBlockSize = (long) (Math.ceil(((finalOutFileSize + SPILL_PADDING) / checksumSize)) * checksumSize);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("merging multiple spills. default block size = " + defaultBlockSize);
                }
                // path, permission, overwrite, buffersize, replication,
                // blocksize, progress
                finalOut = fs.create(finalOutputFile, perm, true, bufSize,
                        defaultRep, defaultBlockSize, reporter);
            } else {
                finalOut = rfs.create(finalOutputFile, true, 4096);
            }

            if (numSpills == 0) {
                // create dummy files
                IndexRecord rec = new IndexRecord();
                WrappedSpillRecord sr = new WrappedSpillRecord(partitions);
                try {
                    for (int i = 0; i < partitions; i++) {
                        long segmentStart = finalOut.getPos();
                        Writer<K, V> writer = new Writer<K, V>(job, finalOut,
                                keyClass, valClass, codec, null);
                        writer.close();
                        rec.startOffset = segmentStart;
                        rec.rawLength = writer.getRawLength();
                        rec.partLength = writer.getCompressedLength();
                        sr.putIndex(rec, i);
                    }
                    sr.writeToFile(finalIndexFile, job);
                } finally {
                    finalOut.close();
                }
                return;
            }
            {
//                sortPhase.addPhases(partitions); // Divide sort phase into
//                                                 // sub-phases
                Merger.considerFinalMergeForProgress();
                
                if ( reportSkewReduce ) {
                    // precompute total merge size for all partitions
                    // mergeFactor. inMem is always 0
                    int mergeFactor = job.getInt(JobContext.IO_SORT_FACTOR, 100);
                    List<Long> segmentList = new ArrayList<Long>(numSpills);
                    long[] mergeSizes = new long[partitions];
                    long totalBytes = 0;
                    for ( int parts = 0; parts < partitions; parts++ ) {
                        segmentList.clear();
                        for ( int i = 0; i < numSpills; ++i ) {
                            IndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);
                            segmentList.add(indexRecord.partLength);
                        }
                        if ( numSpills > mergeFactor ) {
                            Collections.sort(segmentList);
                        }
                        // total bytes
                        mergeSizes[parts] = computeBytesInMerges(mergeFactor,segmentList);
                        totalBytes += mergeSizes[parts];
                    }
                    
                    ((TaskProgress.MapProgress)myProgress).setSizeEstiates(mergeSizes);
                    
                    float[] weights = new float[partitions];
                    for ( int i = 0; i < partitions; ++i ) {
                        weights[i] = (float) (mergeSizes[i] / (double)totalBytes);
                    }
                    
                    sortPhase.addPhases(weights);
                } else {
                    sortPhase.addPhases(partitions);
                }

                IndexRecord rec = new IndexRecord();
                // final SpillRecord spillRec = new SpillRecord(partitions);
                final WrappedSpillRecord spillRec = new WrappedSpillRecord(partitions);
                for (int parts = 0; parts < partitions; parts++) {
                    // create the segments to be merged
                    List<Segment<K, V>> segmentList = new ArrayList<Segment<K, V>>(
                            numSpills);
                    for (int i = 0; i < numSpills; i++) {
                        IndexRecord indexRecord = indexCacheList.get(i)
                                .getIndex(parts);

                        Segment<K, V> s = new Segment<K, V>(job, rfs,
                                filename[i], indexRecord.startOffset,
                                indexRecord.partLength, codec, true);
                        segmentList.add(i, s);

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("MapId=" + mapId + " Reducer=" + parts
                                    + "Spill =" + i + "("
                                    + indexRecord.startOffset + ","
                                    + indexRecord.rawLength + ", "
                                    + indexRecord.partLength + ")");
                        }
                    }

                    int mergeFactor = job.getInt(JobContext.IO_SORT_FACTOR, 100);
                    // sort the segments only if there are intermediate merges
                    boolean sortSegments = segmentList.size() > mergeFactor;
                    // merge
                    @SuppressWarnings("unchecked")
                    RawKeyValueIterator kvIter = Merger.merge(job, rfs,
                            keyClass, valClass, codec, segmentList,
                            mergeFactor, new Path(mapId.toString()),
                            job.getOutputKeyComparator(), reporter,
                            sortSegments, null, spilledRecordsCounter,
                            sortPhase.phase(), myProgress);

                    // write merged output to disk
                    long segmentStart = finalOut.getPos();
                    Writer<K, V> writer = new Writer<K, V>(job, finalOut,
                            keyClass, valClass, codec, spilledRecordsCounter);
                    if (combinerRunner == null
                            || numSpills < minSpillsForCombine) {
                        // FIXME in this case, we know how many tuples are there. easy case!
                        Merger.writeFile(kvIter, writer, reporter, job);
                    } else {
                        combineCollector.setWriter(writer);
                        combinerRunner.combine(kvIter, combineCollector);
                    }
                    
                    // FIXME write out final statistics

                    // close
                    writer.close();
                    kvIter.close();

                    sortPhase.startNextPhase();

                    // record offsets
                    rec.startOffset = segmentStart;
                    rec.rawLength = writer.getRawLength();
                    rec.partLength = writer.getCompressedLength();
                    spillRec.putIndex(rec, parts);
                }
                spillRec.writeToFile(finalIndexFile, job);
                finalOut.close();
                for (int i = 0; i < numSpills; i++) {
                    rfs.delete(filename[i], true);
                }
            }
        }

    } // MapOutputBuffer

    /**
     * Exception indicating that the allocated sort buffer is insufficient to
     * hold the current record.
     */
    @SuppressWarnings("serial")
    private static class MapBufferTooSmallException extends IOException {
        public MapBufferTooSmallException(String s) {
            super(s);
        }
    }
    
    
    private byte[] retrieveSplitInformation(TaskID orgTaskID) throws NullPointerException, HttpException, IOException, InterruptedException {
        String uri = conf.get(SkewTuneJobConfig.ORIGINAL_TASK_TRACKER_HTTP_ATTR) + "/progress?i=" + orgTaskID + "&m=" + getPartition();
        
        LOG.info("retrieving split infomation from "+uri);
        
        URI url = new URI(uri, false);
        HttpClient m_client = new HttpClient();
        HttpMethod method = new GetMethod(url.getEscapedURI());
        method.setRequestHeader("Accept", "*/*");
        
        while ( true ) {
            int respCode = m_client.executeMethod(method);
            if ( respCode == 200 ) {
                // great! we can continue
                break;
            } else if ( respCode == 202 ) {
                // wait. resubmit?
                Thread.sleep(3000); // sleep 3 seconds.
                LOG.info("retry "+uri);
            } else {
                // error. halt.
                LOG.error("can't retrieve split information. resp code = "+respCode);
                throw new IOException("failed to contact http tracker code = "+respCode);
            }
        }
        
        return method.getResponseBody();
    }

    
    // SKEWREDUCE
    InputSplit getInputSplit(final TaskSplitIndex splitIndex) throws NullPointerException, HttpException, IOException, InterruptedException, ClassNotFoundException {
        InputSplit inputSplit = null;
        
        /*
        if ( conf.getBoolean(SkewTuneJobConfig.SKEWTUNE_REACTIVE_JOB, false) ) {
            String orgTaskIDstr = conf.get(SkewTuneJobConfig.ORIGINAL_TASK_ID_ATTR);
            TaskID orgTaskID = TaskID.forName(orgTaskIDstr);
            if ( orgTaskID.getTaskType() == TaskType.MAP ) {
                if ( conf.getBoolean(SkewTuneJobConfig.ENABLE_SKEWTUNE_TAKEOVER_MAP, false) ) {
                    byte[] split = retrieveSplitInformation(orgTaskID);
                    // now construct input split from this
                    DataInputBuffer inBuf = new DataInputBuffer();
                    inBuf.reset(split, split.length);
                    String splitClass = inBuf.readUTF();
                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug("received bytes = "+split.length);
                        LOG.debug("received split class = "+splitClass);
                    }
                    inputSplit = (InputSplit)ReflectionUtils.newInstance(conf.getClassByName(splitClass), conf);
                    inputSplit.readFields(inBuf);
                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug("received split = "+inputSplit);
                    }
                } // otherwise, use default
            } else {
                if ( conf.getBoolean(SkewTuneJobConfig.ENABLE_SKEWTUNE_REDUCE, false) ) {
                    // should contact the task tracker HTTP
                    byte[] resp = retrieveSplitInformation(orgTaskID);
                    String encodedMinKey = Base64.encode(resp);
                    
                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug("received min key bytes = "+resp.length);
                        LOG.debug("base64 encoded key = "+encodedMinKey);
                    }
                    conf.set(SkewTuneJobConfig.REACTIVE_REDUCE_MIN_KEY, encodedMinKey);
                    
                } // use default
            }
        }
        */
        
        if ( inputSplit == null ) {
            inputSplit = getSplitDetails(
                new Path(splitIndex.getSplitLocation()),
                splitIndex.getStartOffset());
        }
        
        return inputSplit;
    }
    
    // from Merger.java -- to compute total merge size.
    
    
    private int getPassFactor(int factor, int passNo, int numSegments) {
        if (passNo > 1 || numSegments <= factor || factor == 1) 
          return factor;
        int mod = (numSegments - 1) % (factor - 1);
        if (mod == 0)
          return factor;
        return mod + 1;
    }
    
    long computeBytesInMerges(int factor, List<Long> segSizes) {
        int numSegments = segSizes.size();
        List<Long> segmentSizes = new ArrayList<Long>(numSegments);
        long totalBytes = 0;
        int n = numSegments;
        // factor for 1st pass
        int f = getPassFactor(factor, 1, n);
        n = numSegments;
   
        segmentSizes.addAll(segSizes);
        
        // If includeFinalMerge is true, allow the following while loop iterate
        // for 1 more iteration. This is to include final merge as part of the
        // computation of expected input bytes of merges
        boolean considerFinalMerge = true;
        
        while (n > f || considerFinalMerge) {
          if (n <=f ) {
            considerFinalMerge = false;
          }
          long mergedSize = 0;
          f = Math.min(f, segmentSizes.size());
          for (int j = 0; j < f; j++) {
            mergedSize += segmentSizes.remove(0);
          }
          totalBytes += mergedSize;
          
          // insert new size into the sorted list
          int pos = Collections.binarySearch(segmentSizes, mergedSize);
          if (pos < 0) {
            pos = -pos-1;
          }
          segmentSizes.add(pos, mergedSize);
          
          n -= (f-1);
          f = factor;
        }

        return totalBytes;
      }
    
    
    
    /**
     * SKEWREDUCE
     */
    @SuppressWarnings("unchecked")
    private <INKEY, INVALUE, OUTKEY, OUTVALUE> void runSkewTuneOldMapper(
            final JobConf job, final TaskSplitIndex splitIndex,
            final TaskUmbilicalProtocol umbilical, TaskReporter reporter)
            throws IOException, InterruptedException, ClassNotFoundException {
//        InputSplit inputSplit = getSplitDetails(
//                new Path(splitIndex.getSplitLocation()),
//                splitIndex.getStartOffset());
        
        if ( myProgress != null ) {
            myProgress.beginSetup();
        }
        
        InputSplit inputSplit = getInputSplit(splitIndex);

        updateJobWithSplit(job, inputSplit);
        reporter.setInputSplit(inputSplit);

        RecordReader<INKEY, INVALUE> rawIn = // open input
        job.getInputFormat().getRecordReader(inputSplit, job, reporter);
        
        final WrappedRecordReader<INKEY, INVALUE> in = new WrappedRecordReader<INKEY,INVALUE>(job, this.getTaskID(), rawIn, reporter, (TaskProgress.MapProgress)myProgress);
        
        int numReduceTasks = conf.getNumReduceTasks();
        LOG.info("numReduceTasks: " + numReduceTasks);
        MapOutputCollector collector = null;
        
        // FIXME check whether we need to build spilt action
        if ( rawIn instanceof SplittableRecordReader ) {
            TellAndStopAction action = new TellAndStopAction() {
                @Override
                public Future<StopStatus> tellAndStop(StopContext context) throws IOException {
                    return in.stop(context);
                }
            };
            
            setupStopHandler(action);
        }

        forceSort = job.getBoolean(SkewTuneJobConfig.MAP_OUTPUT_SORT_ATTR,false);
        if (forceSort) {
            defaultRep = (short) job.getInt(
                    SkewTuneJobConfig.MAP_OUTPUT_REPLICATION_ATTR,
                    job.getInt("dfs.replication", 1));
            defaultWorkFile = FileOutputFormat.getTaskOutputPath(job, getOutputName(getPartition()));
            defaultIndexFile = new Path( defaultWorkFile.getParent(), "." + defaultWorkFile.getName() + ".index");

            LOG.debug("default replication level = " + defaultRep);
            LOG.info("setting default output to " + defaultWorkFile);
            LOG.info("setting default index to " + defaultIndexFile);
        }

        if (forceSort || numReduceTasks > 0) {
            collector = new MapOutputBuffer(umbilical, job, reporter);
        } else {
            collector = new DirectMapOutputCollector(umbilical, job, reporter);
        }
        MapRunnable<INKEY, INVALUE, OUTKEY, OUTVALUE> runner = ReflectionUtils
                .newInstance(job.getMapRunnerClass(), job);

        try {
            OutputCollector<OUTKEY,OUTVALUE> oldCollector = new OldOutputCollector(collector,conf);
            
            if ( myProgress != null ) {
                myProgress.endSetup();
                myProgress.beginCompute();
            }
            
            runner.run(in, oldCollector, reporter);
            mapPhase.complete();
            
            if ( myProgress != null ) {
                myProgress.beginSort();
            }
            
            // wait for scan if there is any
            if ( stopContext != null ) {
                // first we need to wait on stopContext
                LOG.info("wait for pending scanning task");
                final int MAX_WAIT_TIME = 3;
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
            
            if ( myProgress != null ) { // real begin!
                myProgress.beginSort();
            }

            setPhase(TaskStatus.Phase.SORT);
            statusUpdate(umbilical);
            collector.flush();
        } finally {
            // close
            in.close(); // close input
            collector.close();
        }
    }

}
