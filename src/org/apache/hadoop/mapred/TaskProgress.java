package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Progress;

import skewtune.mapreduce.protocol.TaskCostReport;

public abstract class TaskProgress implements MergerProgressListener {
    protected static final Log LOG = LogFactory.getLog(TaskProgress.class);
//    protected final TaskReporter reporter;
    protected final TaskStatus taskStatus;
    
    protected long taskStartAt;
    protected long timeSetup;
    protected long taskEndAt;
    protected long sortStartAt;
    protected long computeStartAt;
    protected long currentPhaseStartAt;

    
    protected final Counter inputBytesCounter;
    protected final Counter inputRecordsCounter;
    protected final Counter outputBytesCounter;
    protected final Counter outputRecordsCounter;
    
    protected final Counter procBytesCounter;
    protected final Counter procRecordsCounter;

    protected final Progress sortPhase;
    
    protected final int sortmb;
    protected final int sortfactor;

    protected volatile boolean stopped = false;
    protected volatile boolean scanning;
    
    protected final ScheduledExecutorService costReporter;
    protected volatile boolean reportCostNow;
    protected TaskCostReport localCostReport;
    protected TaskCostReport costReport;
    
    protected TaskProgress(TaskType type,Configuration conf,Counters counters,TaskStatus status,Progress sortPhase) {
//        this.reporter = reporter;
        this.taskStatus = status;
        this.sortPhase = sortPhase;
        
        inputBytesCounter = counters.findCounter(FileInputFormat.COUNTER_GROUP,FileInputFormat.BYTES_READ);
        procBytesCounter = counters.findCounter("SkewReduce", "PROC_BYTES");
        procRecordsCounter = counters.findCounter("SkewReduce", "PROC_RECORDS");
        
        if ( type == TaskType.MAP ) {
            inputRecordsCounter = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS);
            outputBytesCounter = counters.findCounter(TaskCounter.MAP_OUTPUT_BYTES);
            outputRecordsCounter = counters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS);
        } else {
            inputRecordsCounter = counters.findCounter(TaskCounter.REDUCE_INPUT_RECORDS);
            outputBytesCounter = null;
            outputRecordsCounter = null;
        }
        
        this.sortmb = conf.getInt(MRJobConfig.IO_SORT_MB, 100) << 20;
        this.sortfactor = conf.getInt(MRJobConfig.IO_SORT_FACTOR, 100);
        
        costReporter = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }});
    }
    
    public void start() {
        costReporter.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                reportCostNow = true;
            }}, 60, 60, TimeUnit.SECONDS);
    }
    
    public void shutdown() {
        costReporter.shutdownNow();
    }
    
    public synchronized TaskCostReport getCostReport() {
        TaskCostReport report = this.costReport;
        this.costReport = null;
        return report;
    }
    
    public abstract long getTotalBytes(InputSplit split) throws IOException;
    

    public synchronized void beginTask() {
        currentPhaseStartAt = taskStartAt = System.currentTimeMillis();
    }
    
    public long getStartTime() {
        return taskStartAt;
    }
    
    public synchronized void beginSetup() {
        timeSetup = System.currentTimeMillis();
    }
    
    public synchronized void endSetup() {
        timeSetup = System.currentTimeMillis() - timeSetup;
    }
    
    public long getSetupTime() {
        return timeSetup;
    }
    
    public synchronized void endTask() {
       taskEndAt = System.currentTimeMillis();
       shutdown();
    }
    
    public synchronized void beginSort() {
        currentPhaseStartAt = sortStartAt = System.currentTimeMillis();
    }
    public long getSortStartAt() { return sortStartAt; }

    public synchronized void beginCompute() {
        currentPhaseStartAt = computeStartAt = System.currentTimeMillis();
        this.localCostReport = new TaskCostReport(computeStartAt);
        start();
    }
    
    public long getComputeStartAt() { return computeStartAt; }
    
    public abstract long getRemainingTime(long now);
    public abstract float getProgress();
    public abstract float getTimePerByte(long now);
    public abstract long  getRemainingBytes(InputSplit split);
    public abstract long getTotalComputeTime(long now);
    public synchronized long getTimePassedInPhase(long now) {
        return now - this.currentPhaseStartAt;
    }
    
    /**
     * with this, the processing counters won't be updated
     */
    public void startScan() { scanning = true; }
    public void stopScan() { scanning = false; }
    
    public void stop() {
        stopped = true;
    }
    
    protected double getRemainingTime(double prog,long t) {
        return ( t / Math.max(0.00001, prog) ) - t;
    }
    
    public class SpillStat {
        long duration;
        long inBytes;
        long inRecs;
        long outBytes;
        long outRecs;
        
        public SpillStat(long t,long inBytes,long inRecs,long outBytes,long outRecs) {
            this.duration = t;
            this.inBytes = inBytes;
            this.inRecs = inRecs;
            this.outBytes = outBytes;
            this.outRecs = outRecs;
        }
        
        public long getDuration() { return duration; }
        public long getInputBytes() { return inBytes; }
        public long getInputRecs() { return inRecs; }
        public long getOutputBytes() { return outBytes; }
        public long getOutputRecs() { return outRecs; }
        
        public double getOBperIB() { return outBytes / (double)inBytes; }
        public double getTperIB() { return duration/(inBytes*1000.); }
        public double getORperIR() { return outRecs / (double)inRecs; }
    }
    
    public static class MapProgress extends TaskProgress implements MergerProgressListener {
        // last eval
        long lastEval;
        
        // related to MAP and SPILL
        List<SpillStat> spillStats = new ArrayList<SpillStat>();
        long totalSpilledInputBytes;
        long totalSpilledBytes;
        long totalSpillTime;
        
        double estSortPhase;
        
        // related to SORT
        long cumTotalBytes;
        long cumTotalProcessed;
        
        long[] totalBytesPerPartitions;
        int partition; // currently processing partition
        int numPartitions;
        
        long totalBytesInProgress;
        long bytesProcessedInProgress;
        boolean finalMerge;
        
        final Progress mapPhase;

        public MapProgress(Configuration conf,Counters counters,TaskStatus status,Progress mapPhase, Progress sortPhase) {
            super(TaskType.MAP,conf,counters,status,sortPhase);
            this.mapPhase = mapPhase;
        }
        
        public synchronized void addSpill(long t,long ib,long ir,long ob,long or) {
            spillStats.add(new SpillStat(t,ib,ir,ob,or));
            totalSpilledInputBytes += ib;
            totalSpilledBytes += ob;
            totalSpillTime += t;
        }
        
        public synchronized void setSizeEstiates(long[] ests) {
            totalBytesPerPartitions = ests;
            numPartitions = ests.length;
            cumTotalBytes = 0;
            for ( long sz : ests ) {
                cumTotalBytes += sz;
            }
        }
        
        public synchronized void setNumPartitions(int n) {
            this.numPartitions = n;
        }
        
        public synchronized void beginNewMap() {
            if ( reportCostNow ) {
                costReport = localCostReport.getDelta(System.currentTimeMillis(),procRecordsCounter.getCounter(),procBytesCounter.getCounter());
                reportCostNow = false;
            }
        }

        @Override
        public synchronized void update(long bytes,long total) {
            this.totalBytesInProgress = total;
            this.bytesProcessedInProgress = bytes;
        }

        @Override
        public synchronized void beginFinalMerge(long total, long bytes) {
            finalMerge = true;
        }

        @Override
        public synchronized void setProcessedBytesByKey(long bytes) {
            if ( ! scanning )
                this.bytesProcessedInProgress = bytes;
        }
        
        @Override
        public synchronized void setProcessedBytesByValue(long bytes) {
            if ( ! scanning )
                this.bytesProcessedInProgress = bytes;
        }

        @Override
        public synchronized void endMerge() {
            if ( finalMerge ) {
                finalMerge = false;
                if ( totalBytesPerPartitions != null ) {
                    cumTotalBytes -= bytesProcessedInProgress - totalBytesPerPartitions[partition];
                }
                cumTotalProcessed += bytesProcessedInProgress;
                totalBytesInProgress = 0;
                bytesProcessedInProgress = 0;
                ++partition;
                
                if ( LOG.isDebugEnabled() ) {
                    LOG.debug("cum. total proc = "+cumTotalProcessed+"; cum. total = "+cumTotalBytes);
                }
            }
        }
        
        public synchronized double getSortProgress() {
            if ( totalBytesPerPartitions == null ) {
                return Math.min(1.0,((partition / (double)numPartitions) + (bytesProcessedInProgress/(double)totalBytesInProgress)));
            }
            return ((cumTotalProcessed+bytesProcessedInProgress)/(double)(cumTotalBytes));
        }
        
        public float getProgress() {
            TaskStatus.Phase phase = taskStatus.getPhase();
            if ( phase == TaskStatus.Phase.MAP ) {
                return mapPhase.getProgress();
            } else if ( phase == TaskStatus.Phase.SORT ) {
                return (float)getSortProgress();
            }
            return taskStatus.getProgress();
        }
        
        @Override
        public float getTimePerByte(long now) {
            double timeSoFar = ( sortStartAt > 0 ? sortStartAt : now ) - computeStartAt;
            return (float)((timeSoFar / procBytesCounter.getCounter())*0.001);
        }

        @Override
        public long getRemainingTime(long now) {
            double timeRemain = 0.;
            switch ( taskStatus.getPhase() ) {
                case MAP:
                    {
                        // FIXME
                        // from map progress, infer remaining bytes
                        double timeSoFar = now - computeStartAt;
                        float mapProg = mapPhase.getProgress();
                        long inputSoFar = inputBytesCounter.getCounter();
                        long bytesRemain = (long)(inputSoFar * ((1.0 - mapProg)/mapProg));
                        double timePerBytes = timeSoFar / procBytesCounter.getCounter();
                        
                        int  numSpills;
                        long copyTotalSpilledBytes;
                        long copyTotalSpilledInputBytes;
                        long copyTotalSpillTime;
                        
                        synchronized (this) {
                            numSpills = spillStats.size();
                            copyTotalSpilledBytes = totalSpilledBytes;
                            copyTotalSpilledInputBytes = totalSpilledInputBytes;
                            copyTotalSpillTime = totalSpillTime;
                        }
                        
                        timeRemain = (long)(timePerBytes * bytesRemain);
                        
//                        float mapProg = Math.max(0.00001f,mapPhase.getProgress());
//                        timeRemain = getRemainingTime( mapProg, now - computeStartAt );
                        if ( numSpills == 0 ) {
                            // no spill yet.
                            long outSize = outputBytesCounter.getCounter();
                            long estSize = (long)(outSize / mapProg); // FIXME what if mapProg is zero?
                            int estNumSpills = (int)Math.ceil(estSize / (double)sortmb);
                            double rate = outSize / timeSoFar; // FIXME what if outSize is zero?
                            if ( estNumSpills <= sortfactor ) {
                                estSortPhase =  ( estSize * rate );
                            } else {
                                int passFactor = estNumSpills / sortfactor;
                                estSortPhase = ( (estSize + passFactor*sortmb) * rate );
                            }
                        } else {
                            // we have some spills. 
                            long estSize = (long) (( copyTotalSpilledBytes / (double)copyTotalSpilledInputBytes ) * outputBytesCounter.getCounter() / mapProg);
                            // FIXME what if mapProg is zero?
                            // FIXME what if totalSpilledBytes is zero?
                            
                            long avgSpillSize = totalSpilledBytes / numSpills;
                            
                            int estNumSpills = (int)Math.ceil(estSize / (double)avgSpillSize);
                            double rate = (double)copyTotalSpillTime / copyTotalSpilledBytes;
                            if ( estNumSpills <= sortfactor ) {
                                estSortPhase = (estSize * rate);
                            } else {
                                // we need intermediate merge.
                                int passFactor = estNumSpills / sortfactor;
                                estSortPhase = ( (estSize + passFactor*avgSpillSize*sortfactor) * rate);
                            }
                        }
                        
                        if ( LOG.isDebugEnabled() ) {
                            LOG.debug("map phase time remain = "+timeRemain+"; estSort = "+estSortPhase);
                        }
                        
                        timeRemain += estSortPhase;
                    }
                    break;
                case SORT:
                    {
                    double sortProg = getSortProgress();
                    timeRemain = ( Double.isNaN(sortProg) ) ? estSortPhase - (now - lastEval): getRemainingTime( sortProg, now - sortStartAt);
                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug("sort phase time remain = "+timeRemain+"; sort = "+sortProg);
                    }
                    }
                    break;
            }
            lastEval = now;
            return (long)timeRemain;
        }

        @Override
        public long getRemainingBytes(InputSplit split) {
            try {
                long totalLength = split.getLength();
                return Math.max(0,totalLength - procBytesCounter.getValue());
            } catch ( IOException ex ) {
                ex.printStackTrace();
            }
            return 0;
        }

        @Override
        public long getTotalBytes(InputSplit split) throws IOException {
            return Math.max(split.getLength(),inputBytesCounter.getCounter());
        }

        @Override
        public long getTotalComputeTime(long now) {
            return now - this.taskStartAt;
        }
    }
    
    public static class ReduceProgress extends TaskProgress implements MergerProgressListener {
        long totalReduceBytes;
        
        long totalMergeBytes;
        AtomicLong mergeBytes = new AtomicLong();
        Counters.Counter shuffleBytesCounter;
        Counters.Counter procGroupsCounter;
        
        Progress shufflePhase;
        
        long processedBytes;
        long processingValues;
        long processingBytes;
        
        public ReduceProgress(Configuration conf,Counters counters,TaskStatus status,Progress shufflePhase,Progress sortPhase) {
            super(TaskType.REDUCE,conf,counters,status,sortPhase);
            this.shufflePhase = shufflePhase;
            procGroupsCounter = counters.findCounter("SkewReduce", "PROC_GROUPS");
            shuffleBytesCounter = counters.findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES);
        }

        @Override
        public synchronized void update(long bytes, long total) throws IOException {
            mergeBytes.set(bytes);
            totalMergeBytes = total;
            if ( stopped )
                throw new InterruptedIOException();
        }

        @Override
        public synchronized void beginFinalMerge(long total, long bytes) {
            // we will only keep track of final merge.
            mergeBytes.set(bytes);
            totalReduceBytes = total;
            processedBytes = bytes;

            if ( LOG.isDebugEnabled() ) {
                LOG.debug("begin final merge. reduce byte base = "+processedBytes+"; reduce total bytes = "+totalReduceBytes);
            }
        }

        @Override
        public synchronized void setProcessedBytesByKey(long bytes) throws IOException {
            if ( processedBytes > 0 ) {
                if ( ! scanning ) {
                    long diff = bytes - processedBytes;
                    
                    processingBytes += diff;
                    inputBytesCounter.increment(diff);
                    
                    processedBytes = bytes;
                }
            } else {
                mergeBytes.set(bytes);
                if ( stopped )
                    throw new InterruptedIOException();
            }
        }
        
        @Override
        public synchronized void setProcessedBytesByValue(long bytes) throws IOException {
            if ( processedBytes > 0 ) {
                if ( ! scanning ) {
                    long diff = bytes - processedBytes;
                    
                    processingBytes += diff;
                    inputBytesCounter.increment(diff);
                    ++processingValues;
                    
                    processedBytes = bytes;
                }
            } else {
                mergeBytes.set(bytes);
                if ( stopped )
                    throw new InterruptedIOException();
            }
        }

        @Override
        public void endMerge() {
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("end of a merge...");
            }
        }
        
        
        public synchronized void beginNewReduce() {
            if ( processingValues > 0 ) {
                procGroupsCounter.increment(1);
                procBytesCounter.increment(processingBytes);
                procRecordsCounter.increment(processingValues);
                
                if ( reportCostNow ) {
                    // we have just processed one group. compute delta
                    costReport = localCostReport.getDelta(System.currentTimeMillis(),procRecordsCounter.getCounter(),procBytesCounter.getCounter(),procGroupsCounter.getCounter());
                    reportCostNow = false;
                }
                
                processingBytes = 0;
                processingValues = 0;
            }
        }
        
        public float getProgress() {
            TaskStatus.Phase phase = taskStatus.getPhase();
            if ( phase == TaskStatus.Phase.REDUCE ) {
                return (float) (inputBytesCounter.getValue() / (double)totalReduceBytes);
            } else if ( phase == TaskStatus.Phase.SHUFFLE ) {
                return shufflePhase.getProgress();
            } else if ( phase == TaskStatus.Phase.SORT ) {
//                return (float) (mergeBytes.get() / (double)totalMergeBytes);
                return sortPhase.getProgress();
            }
            return taskStatus.getProgress();
        }
        
        
        @Override
        public synchronized long getRemainingTime(long now) {
            double timeRemain = 0.;
            switch ( taskStatus.getPhase() ) {
                case SHUFFLE:
                {
                    // fallback to default for simplicity
                    long timeSoFar = now - taskStartAt;
                    timeRemain = getRemainingTime( shufflePhase.getProgress(), timeSoFar );
                    timeRemain += ((timeSoFar + timeRemain) * 2);
                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug("shuffle phase time remain = "+timeRemain);
                    }
                    break;
                }
                case SORT:
                {
                    // assuming sort
                    long timeSoFar = now - sortStartAt;
//                    if ( totalMergeBytes == 0 ) {
//                        timeRemain = 0;
//                    } else {
//                        timeRemain = getRemainingTime( (mergeBytes.get() / (double)totalMergeBytes), timeSoFar);
//                        timeRemain += (timeSoFar + timeRemain);
//                    }
                    timeRemain = getRemainingTime( sortPhase.getProgress(), timeSoFar );
                    timeRemain += (timeSoFar + timeRemain);
                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug("sort phase time remain = "+timeRemain);
                    }
                    break;
                }
                case REDUCE:
                {
                    timeRemain = (( totalReduceBytes - inputBytesCounter.getValue())/(double)(procBytesCounter.getCounter() + processingBytes)) * (now - computeStartAt);
//                    timeRemain = getRemainingTime( (inputBytesCounter.getValue() / (double)totalReduceBytes), now - computeStartAt);
                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug("reduce phase time remain = "+timeRemain);
                    }
                }
            }
            return (long)timeRemain;
        }

        @Override
        public float getTimePerByte(long now) {
            // FIXME what to return if it's SHUFFLE or SORT?
            if ( computeStartAt == 0. ) {
//                throw new IllegalStateException("not in running reduce! : "+taskStatus.getPhase());
                return 0.f;
            }
            double timeSoFar = now - computeStartAt;
            float v = (float)((timeSoFar / (procBytesCounter.getCounter() + processingBytes))*0.001);
            if ( Float.isNaN(v) ) {
                LOG.warn("time per byte is NaN!");
                v = 0.f;
            }
            return v;
        }

        @Override
        public long getRemainingBytes(InputSplit split) {
            // FIXME what to return if it's SHUFFLE or SORT?
            TaskStatus.Phase phase = taskStatus.getPhase();
            if ( phase == TaskStatus.Phase.REDUCE ) {
                return totalReduceBytes - inputBytesCounter.getValue();
            }
            return (long)(shuffleBytesCounter.getValue() / shufflePhase.getProgress());
        }
        
        @Override
        public long getTotalBytes(InputSplit split) {
            return totalReduceBytes;
        }
        
        /**
         * bytes per seconds
         * @return
         */
        public double getSortSpeed() {
            return mergeBytes.get() / (( computeStartAt - sortStartAt )*0.001);
        }

        @Override
        public long getTotalComputeTime(long now) {
            return now - this.sortStartAt;
        }
    }

    public synchronized TaskCostReport getLastUpdate() {
        return localCostReport == null ? null : localCostReport.getLastReport(computeStartAt);
    }
}
