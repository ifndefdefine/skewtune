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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.reduce.MapHost.State;
import org.apache.hadoop.util.Progress;

import skewtune.mapreduce.SkewTuneJobConfig;
import skewtune.mapreduce.TaskState;
import skewtune.mapreduce.protocol.TaskStatusEvent.Internal;

class ShuffleScheduler<K, V> {
    static ThreadLocal<Long> shuffleStart = new ThreadLocal<Long>() {
        protected Long initialValue() {
            return 0L;
        }
    };

    private static final Log LOG = LogFactory.getLog(ShuffleScheduler.class);

    private static final int MAX_MAPS_AT_ONCE = 20;
    private static final long INITIAL_PENALTY = 10000;
    private static final float PENALTY_GROWTH_RATE = 1.3f;
    private final static int REPORT_FAILURE_LIMIT = 10;

    private final boolean[] finishedMaps;

    private final int totalMaps;

    private int remainingMaps;

    private Map<String, MapHost> mapLocations = new HashMap<String, MapHost>();
    private Set<MapHost> pendingHosts = new HashSet<MapHost>();
    private Set<TaskAttemptID> obsoleteMaps = new HashSet<TaskAttemptID>();

    private final Random random = new Random(System.currentTimeMillis());

    private final DelayQueue<Penalty> penalties = new DelayQueue<Penalty>();

    private final Referee referee = new Referee();
    private final Map<TaskAttemptID, IntWritable> failureCounts = new HashMap<TaskAttemptID, IntWritable>();
    private final Map<String, IntWritable> hostFailures = new HashMap<String, IntWritable>();

    private final TaskStatus status;

    private final ExceptionReporter reporter;

    private final int abortFailureLimit;

    private final Progress progress;

    private final Counters.Counter shuffledMapsCounter;
    private final Counters.Counter reduceShuffleBytes;
    private final Counters.Counter failedShuffleCounter;

    private final long startTime;

    private long lastProgressTime;

    private int maxMapRuntime = 0;

    private int maxFailedUniqueFetches = 5;

    private int maxFetchFailuresBeforeReporting;

    private long totalBytesShuffledTillNow = 0;

    private DecimalFormat mbpsFormat = new DecimalFormat("0.00");

    private boolean reportReadErrorImmediately = true;
    
    private final boolean reactiveShuffle;
    private final boolean isReactiveJob;

    public ShuffleScheduler(JobConf job, TaskStatus status,
            ExceptionReporter reporter, Progress progress,
            Counters.Counter shuffledMapsCounter,
            Counters.Counter reduceShuffleBytes,
            Counters.Counter failedShuffleCounter,
            boolean reactiveShuffle) {
        totalMaps = job.getNumMapTasks();
        abortFailureLimit = Math.max(30, totalMaps / 10);
        remainingMaps = totalMaps;
        finishedMaps = new boolean[remainingMaps];
        this.reporter = reporter;
        this.status = status;
        this.progress = progress;
        this.shuffledMapsCounter = shuffledMapsCounter;
        this.reduceShuffleBytes = reduceShuffleBytes;
        this.failedShuffleCounter = failedShuffleCounter;
        this.startTime = System.currentTimeMillis();
        lastProgressTime = startTime;
        referee.start();
        this.maxFailedUniqueFetches = Math.min(totalMaps,
                this.maxFailedUniqueFetches);
        this.maxFetchFailuresBeforeReporting = job.getInt(
                MRJobConfig.SHUFFLE_FETCH_FAILURES, REPORT_FAILURE_LIMIT);
        this.reportReadErrorImmediately = job.getBoolean(
                MRJobConfig.SHUFFLE_NOTIFY_READERROR, true);
        
        reactiveInProgress = new BitSet(totalMaps);
        reactiveMapOutput = new BitSet(totalMaps);
        
        orgMapOutput = new BitSet(totalMaps);
        this.takeoverState = new TaskState[totalMaps];
        Arrays.fill(takeoverState, TaskState.RUNNING);
        
        this.reactiveShuffle = reactiveShuffle;
        this.isReactiveJob = job.getBoolean(SkewTuneJobConfig.SKEWTUNE_REACTIVE_JOB, false);
    }

    public synchronized void copySucceeded(TaskAttemptID mapId, MapHost host,
            long bytes, long millis, MapOutput<K, V> output) throws IOException {
        failureCounts.remove(mapId);
        hostFailures.remove(host.getHostName());
        int mapIndex = mapId.getTaskID().getId();
        
        // FIXME if this reduce belongs to reactive reduce, we don't need to care about other reactive outputs
        // unless we support recursive reactive jobs

        if ( reactiveShuffle && ! isReactiveJob ) {
            // neither finished by other shuffle task nor finished by the reactive map output
            if ( finishedMaps[mapIndex] || reactiveInProgress.get(mapIndex) ) {
                // reactive speculative execution completed sooner
                output.abort();
            } else {
                this.orgMapOutput.set(mapIndex);
                TaskState state = takeoverState[mapIndex];
                output.commit();
                
                totalBytesShuffledTillNow += bytes;
                reduceShuffleBytes.increment(bytes);
                lastProgressTime = System.currentTimeMillis();
    
                if ( state == TaskState.COMPLETE || state == TaskState.CANCEL_TAKEOVER ) {
                    // good. we are all DONE!
                    finishedMaps[mapIndex] = true;
                    shuffledMapsCounter.increment(1);
    
                    if (!this.scheduledReactiveMaps.isEmpty()) {
                        // the original task completed before reactive job. cancel.
                        ReactiveMapOutput<K, V> reactive = scheduledReactiveMaps.remove(mapId.getTaskID());
                        if (reactive != null) {
                            reactive.abortAll();
                        }
                    }
    
                    if (--remainingMaps == 0) {
                        notifyAll();
                    }
    
                    // update the status
                    float mbs = (float) totalBytesShuffledTillNow / (1024 * 1024);
                    int mapsDone = totalMaps - remainingMaps;
                    long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
    
                    float transferRate = mbs / secsSinceStart;
                    progress.set((float) mapsDone / totalMaps);
                    String statusString = mapsDone + " / " + totalMaps + " copied.";
                    status.setStateString(statusString);
                    progress.setStatus("copy(" + mapsDone + " of " + totalMaps + " at "
                            + mbpsFormat.format(transferRate) + " MB/s)");
    
                    LOG.debug("map " + mapId + " done " + statusString);
                } else if ( state == TaskState.TAKEOVER ) {
                    // check whether reactive output is done or not.
                    if ( this.reactiveMapOutput.get(mapIndex) ) {
                        // yay! we are done!
                        finishedMaps[mapIndex] = true;
                        shuffledMapsCounter.increment(1);
                        if ( --remainingMaps == 0 ) {
                            notifyAll();
                        }
                        // update the status
                        float mbs = (float) totalBytesShuffledTillNow / (1024 * 1024);
                        int mapsDone = totalMaps - remainingMaps;
                        long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
    
                        float transferRate = mbs / secsSinceStart;
                        progress.set((float) mapsDone / totalMaps);
                        String statusString = mapsDone + " / " + totalMaps + " copied.";
                        status.setStateString(statusString);
                        progress.setStatus("copy(" + mapsDone + " of " + totalMaps + " at "
                                + mbpsFormat.format(transferRate) + " MB/s)");
    
                        LOG.debug("map " + mapId + " done with takeover " + statusString);                    
                    } else {
                        // should wait for reactive output
                        LOG.info("map " + mapId + " done. waiting for reactive output.");
                    }
                } else {
                    // otherwise, do nothing.
                    LOG.info("map " + mapId + " done. waiting for confirmation. state = " + state);
                }
            }
        } else {
            output.commit();
            finishedMaps[mapIndex] = true;
            shuffledMapsCounter.increment(1);

            if (--remainingMaps == 0) {
                notifyAll();
            }

            // update the status
            totalBytesShuffledTillNow += bytes;
            float mbs = (float) totalBytesShuffledTillNow / (1024 * 1024);
            int mapsDone = totalMaps - remainingMaps;
            long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

            float transferRate = mbs / secsSinceStart;
            progress.set((float) mapsDone / totalMaps);
            String statusString = mapsDone + " / " + totalMaps + " copied.";
            status.setStateString(statusString);
            progress.setStatus("copy(" + mapsDone + " of " + totalMaps + " at "
                    + mbpsFormat.format(transferRate) + " MB/s)");

            reduceShuffleBytes.increment(bytes);
            lastProgressTime = System.currentTimeMillis();
            LOG.debug("map " + mapId + " done " + statusString);
        }

        /*
        // neither finished by other shuffle task nor finished by the reactive map output
        if (!finishedMaps[mapIndex] && !reactiveInProgress.get(mapId.getTaskID().getId())) {
            output.commit();
            finishedMaps[mapIndex] = true;
            shuffledMapsCounter.increment(1);

            if (!this.scheduledReactiveMaps.isEmpty()) {
                // the original task completed before reactive job. cancel.
                ReactiveMapOutput<K, V> reactive = scheduledReactiveMaps.remove(mapId.getTaskID());
                if (reactive != null) {
                    reactive.abortAll();
                }
            }

            if (--remainingMaps == 0) {
                notifyAll();
            }

            // update the status
            totalBytesShuffledTillNow += bytes;
            float mbs = (float) totalBytesShuffledTillNow / (1024 * 1024);
            int mapsDone = totalMaps - remainingMaps;
            long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

            float transferRate = mbs / secsSinceStart;
            progress.set((float) mapsDone / totalMaps);
            String statusString = mapsDone + " / " + totalMaps + " copied.";
            status.setStateString(statusString);
            progress.setStatus("copy(" + mapsDone + " of " + totalMaps + " at "
                    + mbpsFormat.format(transferRate) + " MB/s)");

            reduceShuffleBytes.increment(bytes);
            lastProgressTime = System.currentTimeMillis();
            LOG.debug("map " + mapId + " done " + statusString);
        } else {
            // the reactive task has completed first
            output.abort(); // reclaim space
        }
        */

    }

    public synchronized void copyFailed(TaskAttemptID mapId, MapHost host, boolean readError) {
        host.penalize();
        int failures = 1;
        if (failureCounts.containsKey(mapId)) {
            IntWritable x = failureCounts.get(mapId);
            x.set(x.get() + 1);
            failures = x.get();
        } else {
            failureCounts.put(mapId, new IntWritable(1));
        }
        String hostname = host.getHostName();
        if (hostFailures.containsKey(hostname)) {
            IntWritable x = hostFailures.get(hostname);
            x.set(x.get() + 1);
        } else {
            hostFailures.put(hostname, new IntWritable(1));
        }
        if (failures >= abortFailureLimit) {
            try {
                throw new IOException(failures + " failures downloading "
                        + mapId);
            } catch (IOException ie) {
                reporter.reportException(ie);
            }
        }

        checkAndInformJobTracker(failures, mapId, readError);

        checkReducerHealth();

        long delay = (long) (INITIAL_PENALTY * Math.pow(PENALTY_GROWTH_RATE,
                failures));

        penalties.add(new Penalty(host, delay));

        failedShuffleCounter.increment(1);
    }

    // Notify the JobTracker
    // after every read error, if 'reportReadErrorImmediately' is true or
    // after every 'maxFetchFailuresBeforeReporting' failures
    private void checkAndInformJobTracker(int failures, TaskAttemptID mapId,
            boolean readError) {
        if ((reportReadErrorImmediately && readError)
                || ((failures % maxFetchFailuresBeforeReporting) == 0)) {
            LOG.info("Reporting fetch failure for " + mapId + " to jobtracker.");
            status.addFetchFailedMap((org.apache.hadoop.mapred.TaskAttemptID) mapId);
        }
    }

    private void checkReducerHealth() {
        final float MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT = 0.5f;
        final float MIN_REQUIRED_PROGRESS_PERCENT = 0.5f;
        final float MAX_ALLOWED_STALL_TIME_PERCENT = 0.5f;

        long totalFailures = failedShuffleCounter.getValue();
        int doneMaps = totalMaps - remainingMaps;

        boolean reducerHealthy = (((float) totalFailures / (totalFailures + doneMaps)) < MAX_ALLOWED_FAILED_FETCH_ATTEMPT_PERCENT);

        // check if the reducer has progressed enough
        boolean reducerProgressedEnough = (((float) doneMaps / totalMaps) >= MIN_REQUIRED_PROGRESS_PERCENT);

        // check if the reducer is stalled for a long time
        // duration for which the reducer is stalled
        int stallDuration = (int) (System.currentTimeMillis() - lastProgressTime);

        // duration for which the reducer ran with progress
        int shuffleProgressDuration = (int) (lastProgressTime - startTime);

        // min time the reducer should run without getting killed
        int minShuffleRunDuration = (shuffleProgressDuration > maxMapRuntime) ? shuffleProgressDuration
                : maxMapRuntime;

        boolean reducerStalled = (((float) stallDuration / minShuffleRunDuration) >= MAX_ALLOWED_STALL_TIME_PERCENT);

        // kill if not healthy and has insufficient progress
        if ((failureCounts.size() >= maxFailedUniqueFetches || failureCounts
                .size() == (totalMaps - doneMaps))
                && !reducerHealthy
                && (!reducerProgressedEnough || reducerStalled)) {
            LOG.fatal("Shuffle failed with too many fetch failures "
                    + "and insufficient progress!");
            String errorMsg = "Exceeded MAX_FAILED_UNIQUE_FETCHES; bailing-out.";
            reporter.reportException(new IOException(errorMsg));
        }

    }

    public synchronized void tipFailed(TaskID taskId) {
        finishedMaps[taskId.getId()] = true;
    }

    public synchronized void addKnownMapOutput(String hostName, String hostUrl,
            TaskAttemptID mapId) {
        MapHost host = mapLocations.get(hostName);
        if (host == null) {
            host = new MapHost(hostName, hostUrl);
            mapLocations.put(hostName, host);
        }
        host.addKnownMap(mapId);

        // Mark the host as pending
        if (host.getState() == State.PENDING) {
            pendingHosts.add(host);
            notifyAll();
        }
    }

    public synchronized void obsoleteMapOutput(TaskAttemptID mapId) {
        obsoleteMaps.add(mapId);
    }

    public synchronized void putBackKnownMapOutput(MapHost host,
            TaskAttemptID mapId) {
        host.addKnownMap(mapId);
    }

    public synchronized MapHost getHost() throws InterruptedException {
        while (pendingHosts.isEmpty()) {
            wait();
        }

        MapHost host = null;
        Iterator<MapHost> iter = pendingHosts.iterator();
        int numToPick = random.nextInt(pendingHosts.size());
        for (int i = 0; i <= numToPick; ++i) {
            host = iter.next();
        }

        pendingHosts.remove(host);
        host.markBusy();

        LOG.info("Assiging " + host + " with " + host.getNumKnownMapOutputs()
                + " to " + Thread.currentThread().getName());
        shuffleStart.set(System.currentTimeMillis());

        return host;
    }

    public synchronized List<TaskAttemptID> getMapsForHost(MapHost host) {
        List<TaskAttemptID> list = host.getAndClearKnownMaps();
        Iterator<TaskAttemptID> itr = list.iterator();
        List<TaskAttemptID> result = new ArrayList<TaskAttemptID>();
        int includedMaps = 0;
        int totalSize = list.size();
        // find the maps that we still need, up to the limit
        while (itr.hasNext()) {
            TaskAttemptID id = itr.next();
            if (!obsoleteMaps.contains(id)
                    && !orgMapOutput.get(id.getTaskID().getId())
                    && !finishedMaps[id.getTaskID().getId()]
                    && !reactiveInProgress.get(id.getTaskID().getId()) ) {
                result.add(id);
                if (++includedMaps >= MAX_MAPS_AT_ONCE) {
                    break;
                }
            }
        }
        // put back the maps left after the limit
        while (itr.hasNext()) {
            TaskAttemptID id = itr.next();
            if (!obsoleteMaps.contains(id)
                    && !orgMapOutput.get(id.getTaskID().getId())
                    && !finishedMaps[id.getTaskID().getId()]
                    && !reactiveInProgress.get(id.getTaskID().getId()) ) {
                host.addKnownMap(id);
            }
        }
        LOG.info("assigned " + includedMaps + " of " + totalSize + " to "
                + host + " to " + Thread.currentThread().getName());
        return result;
    }

    public synchronized void freeHost(MapHost host) {
        if (host.getState() != State.PENALIZED) {
            if (host.markAvailable() == State.PENDING) {
                pendingHosts.add(host);
                notifyAll();
            }
        }
        LOG.info(host + " freed by " + Thread.currentThread().getName()
                + " in " + (System.currentTimeMillis() - shuffleStart.get())
                + "s");
    }

    public synchronized void resetKnownMaps() {
        mapLocations.clear();
        obsoleteMaps.clear();
        pendingHosts.clear();
    }

    /**
     * Wait until the shuffle finishes or until the timeout.
     * 
     * @param millis
     *            maximum wait time
     * @return true if the shuffle is done
     * @throws InterruptedException
     */
    public synchronized boolean waitUntilDone(int millis)
            throws InterruptedException {
        if (remainingMaps > 0) {
            wait(millis);
            return remainingMaps == 0;
        }
        return true;
    }

    /**
     * A structure that records the penalty for a host.
     */
    private static class Penalty implements Delayed {
        MapHost host;

        private long endTime;

        Penalty(MapHost host, long delay) {
            this.host = host;
            this.endTime = System.currentTimeMillis() + delay;
        }

        public long getDelay(TimeUnit unit) {
            long remainingTime = endTime - System.currentTimeMillis();
            return unit.convert(remainingTime, TimeUnit.MILLISECONDS);
        }

        public int compareTo(Delayed o) {
            long other = ((Penalty) o).endTime;
            return endTime == other ? 0 : (endTime < other ? -1 : 1);
        }

    }

    /**
     * A thread that takes hosts off of the penalty list when the timer expires.
     */
    private class Referee extends Thread {
        public Referee() {
            setName("ShufflePenaltyReferee");
            setDaemon(true);
        }

        public void run() {
            try {
                while (true) {
                    // take the first host that has an expired penalty
                    MapHost host = penalties.take().host;
                    synchronized (ShuffleScheduler.this) {
                        if (host.markAvailable() == MapHost.State.PENDING) {
                            pendingHosts.add(host);
                            ShuffleScheduler.this.notifyAll();
                        }
                    }
                }
            } catch (InterruptedException ie) {
                return;
            } catch (Throwable t) {
                reporter.reportException(t);
            }
        }
    }

    public void close() throws InterruptedException {
        referee.interrupt();
        referee.join();
    }

    public synchronized void informMaxMapRunTime(int duration) {
        if (duration > maxMapRuntime) {
            maxMapRuntime = duration;
        }
    }

    // SKEWREDUCE

    public class NextFetch {
        final MapHost mapHost;

        final FetchTask<K, V> task;

        public NextFetch(MapHost host) {
            mapHost = host;
            task = null;
        }

        public NextFetch(FetchTask task) {
            mapHost = null;
            this.task = task;
        }

        public boolean hasMapHost() {
            return mapHost != null;
        }

        public MapHost getMapHost() {
            return mapHost;
        }

        public FetchTask<K, V> getFetchTask() {
            return task;
        }
    }

    HashMap<TaskID, ReactiveMapOutput<K, V>> scheduledReactiveMaps = new HashMap<TaskID, ReactiveMapOutput<K, V>>();
    HashSet<ReactiveMapOutput<K,V>> workingReactiveMaps = new HashSet<ReactiveMapOutput<K,V>>();
    HashSet<FetchTask<K, V>> pendingReactiveFetch = new HashSet<FetchTask<K, V>>();
    HashMap<TaskID, ReactiveMapOutput<K,V>> obsoleteReactiveMaps = new HashMap<TaskID, ReactiveMapOutput<K,V>>();
    
    final BitSet orgMapOutput;
    final BitSet reactiveMapOutput;
    BitSet reactiveInProgress;
    final int MAX_CONCURRENT_REACTIVE_MAP = 1;
    final TaskState[] takeoverState;

    public synchronized void addKnownReactiveMapOutput(
            ReactiveMapOutput<K, V> rmapout) {
        scheduledReactiveMaps.put(rmapout.getTaskID(), rmapout);
//        pendingReactiveFetch.addAll(rmapout.getAllTasks());
        notifyAll();
    }

    public synchronized void putBackKnownReactiveMapOutput(FetchTask<K, V> task) {
        pendingReactiveFetch.add(task);
    }
    
    private <T> T choose(Collection<T> col) {
        T ret = null;
        Iterator<T> i = col.iterator();
        int numToPick = random.nextInt(col.size());
        for ( int j = 0; j <= numToPick; ++j ) {
            ret = i.next();
        }
        return ret;
    }

    public synchronized ShuffleScheduler<K, V>.NextFetch getNext()
            throws InterruptedException {
        while (true) {
            while (pendingHosts.isEmpty() && pendingReactiveFetch.isEmpty()
                    && ( scheduledReactiveMaps.isEmpty() || workingReactiveMaps.size() >= MAX_CONCURRENT_REACTIVE_MAP ) ) {
                // pending
                wait();
            }

            // fetch one at a time
//            if ( pendingReactiveFetch.isEmpty()
//                    && workingReactiveMaps.size() < MAX_CONCURRENT_REACTIVE_MAP
//                    && scheduledReactiveMaps.size() > 0 ) {
//                TaskID taskid = choose(scheduledReactiveMaps.keySet());
//                ReactiveMapOutput<K,V> rmapout = scheduledReactiveMaps.remove(taskid);
//                
//                workingReactiveMaps.add(rmapout);
//                pendingReactiveFetch.addAll( rmapout.getAllTasks() );
//                
//                notifyAll();
//                
//                LOG.info("starting to work on task "+taskid);
//            }

            if ( pendingReactiveFetch.isEmpty()
                    && workingReactiveMaps.size() < MAX_CONCURRENT_REACTIVE_MAP ) {
                while ( !scheduledReactiveMaps.isEmpty() ) {
                    TaskID taskid = choose(scheduledReactiveMaps.keySet());
                    ReactiveMapOutput<K,V> rmapout = scheduledReactiveMaps.remove(taskid);
                    if ( finishedMaps[taskid.getId()] ) {
                        this.obsoleteReactiveMaps.put(taskid, rmapout);
                        if ( LOG.isInfoEnabled() ) {
                            LOG.info("reactive map output is obsoleted: "+taskid);
                        }
                    } else {
                        // workable!
                        workingReactiveMaps.add(rmapout);
                        pendingReactiveFetch.addAll( rmapout.getAllTasks() );
                        
                        notifyAll();
                        
                        LOG.info("starting to work on task "+taskid);
                        break;
                    }
                }
            }

            
            while (!pendingReactiveFetch.isEmpty()) {
                // retrieve next map output
                FetchTask<K, V> fetchTask = choose(pendingReactiveFetch);
                pendingReactiveFetch.remove(fetchTask);
                
                if (fetchTask.isAborted() ) {
                    continue;
                } else if ( finishedMaps[fetchTask.getMapID()]) {
                    fetchTask.abort();
                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug("already retrieved. cancel this task "+fetchTask);
                    }
                    continue;
                } else {
                    fetchTask.markBusy();

                    LOG.info("Assigning reactive output " + fetchTask + " to "
                            + Thread.currentThread().getName());
                    shuffleStart.set(System.currentTimeMillis());

                    return new NextFetch(fetchTask);
                }
            }

            if (!pendingHosts.isEmpty()) {
                MapHost host = null;
                Iterator<MapHost> iter = pendingHosts.iterator();
                int numToPick = random.nextInt(pendingHosts.size());
                for (int i = 0; i <= numToPick; ++i) {
                    host = iter.next();
                }

                pendingHosts.remove(host);
                host.markBusy();

                LOG.info("Assigning " + host + " with "
                        + host.getNumKnownMapOutputs() + " to "
                        + Thread.currentThread().getName());
                shuffleStart.set(System.currentTimeMillis());

                return new NextFetch(host);
            }
        }
    }

    public synchronized void copySucceeded(TaskAttemptID mapId,
            FetchTask<K, V> task, long millis) throws IOException {
        ReactiveMapOutput.State done = task.readyToCommit();

        // now, commit this reactive output
        int mapIndex = mapId.getTaskID().getId();
        if ( !finishedMaps[mapIndex] ) {
            LOG.info("map " + mapId + " is about to commit");
            if ( done == ReactiveMapOutput.State.COMMIT ) {
                LOG.info("map " + mapId + " commit");
                boolean finished = task.commit();
                
                if ( finished ) {
                    reactiveMapOutput.set(mapIndex);
                    
                    ReactiveMapOutput<K,V> rmapout = task.getReactiveMapOutput();
                    totalBytesShuffledTillNow += rmapout.getTotalSize();
                    reduceShuffleBytes.increment(rmapout.getCompressedTotalSize());
                    lastProgressTime = System.currentTimeMillis();

                    workingReactiveMaps.remove(rmapout);

                    if ( task.isSpeculative() ) {
                        // set finishedMaps
                        // decrement remaining maps
                        finishedMaps[mapIndex] = true;
                        
                        shuffledMapsCounter.increment(1);
                        if (--remainingMaps == 0) {
                            notifyAll();
                        }
        
        //                ReactiveMapOutput<K, V> rmapout = scheduledReactiveMaps.remove(mapId.getTaskID());
        //                workingReactiveMaps.remove(mapId.getTaskID());
                        notifyAll();
                        
                        // update the status
                        float mbs = (float) totalBytesShuffledTillNow / (1024 * 1024);
                        int mapsDone = totalMaps - remainingMaps;
                        long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
            
                        float transferRate = mbs / secsSinceStart;
                        progress.set((float) mapsDone / totalMaps);
                        String statusString = mapsDone + " / " + totalMaps + " copied.";
                        status.setStateString(statusString);
                        progress.setStatus("copy(" + mapsDone + " of " + totalMaps + " at "
                                + mbpsFormat.format(transferRate) + " MB/s)");
            
                        LOG.info("map " + mapId + " done by reactive map " + statusString);
                    } else {
                        // has original one received?
                        if ( this.orgMapOutput.get(mapIndex) ) {
                            // we're done! commit!
                            finishedMaps[mapIndex] = true;
                            
                            shuffledMapsCounter.increment(1);
                            if (--remainingMaps == 0) {
                                notifyAll();
                            }
            
            //                ReactiveMapOutput<K, V> rmapout = scheduledReactiveMaps.remove(mapId.getTaskID());
            //                workingReactiveMaps.remove(mapId.getTaskID());
                            notifyAll();
                            
                            // update the status
                            float mbs = (float) totalBytesShuffledTillNow / (1024 * 1024);
                            int mapsDone = totalMaps - remainingMaps;
                            long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
                
                            float transferRate = mbs / secsSinceStart;
                            progress.set((float) mapsDone / totalMaps);
                            String statusString = mapsDone + " / " + totalMaps + " copied.";
                            status.setStateString(statusString);
                            progress.setStatus("copy(" + mapsDone + " of " + totalMaps + " at "
                                    + mbpsFormat.format(transferRate) + " MB/s)");
                
                            LOG.info("map " + mapId + " done by takeover map " + statusString);
                        } else {
                            // otherwise, we should wait for the original.
                            LOG.info("map " + mapId + " done by takeover map. waiting for original map. state = "+takeoverState[mapIndex]);
                        }
                    }
                } else {
                    LOG.info("reactive map " + mapId + " has not yet finished");
                }
            } else {
                LOG.info("map " + mapId + " is pending commit");
            }
                /*
                finishedMaps[mapIndex] = task.commit(); // now commit all
                
                if ( finishedMaps[mapIndex] ) {
                    shuffledMapsCounter.increment(1);
                    if (--remainingMaps == 0) {
                        notifyAll();
                    }
    
    //                ReactiveMapOutput<K, V> rmapout = scheduledReactiveMaps.remove(mapId.getTaskID());
    //                workingReactiveMaps.remove(mapId.getTaskID());
                    ReactiveMapOutput<K,V> rmapout = task.getReactiveMapOutput();
                    workingReactiveMaps.remove(rmapout);
                    notifyAll();
                    
                    // update the status
                    totalBytesShuffledTillNow += rmapout.getTotalSize();
                    float mbs = (float) totalBytesShuffledTillNow / (1024 * 1024);
                    int mapsDone = totalMaps - remainingMaps;
                    long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
        
                    float transferRate = mbs / secsSinceStart;
                    progress.set((float) mapsDone / totalMaps);
                    String statusString = mapsDone + " / " + totalMaps + " copied.";
                    status.setStateString(statusString);
                    progress.setStatus("copy(" + mapsDone + " of " + totalMaps + " at "
                            + mbpsFormat.format(transferRate) + " MB/s)");
        
                    reduceShuffleBytes.increment(rmapout.getCompressedTotalSize());
                    lastProgressTime = System.currentTimeMillis();
                    LOG.info("map " + mapId + " done by reactive map " + statusString);
                }
            } else {
                LOG.info("map " + mapId + " is pending commit");
            }
            */
        } else {
            ReactiveMapOutput<K, V> rmapout = task.getReactiveMapOutput();
            if (rmapout != null) {
                rmapout.abortAll();
            }
            workingReactiveMaps.remove(rmapout);
            notifyAll();

            LOG.info("Cancelling all reactive tasks since original task shuffled first " + mapId);
        }
    }

    public synchronized void copyFailed(TaskAttemptID mapId,
            FetchTask<K, V> task, boolean readError) {
        int mapIndex = mapId.getTaskID().getId();
        boolean abort = true;
        if ( this.reactiveInProgress.get(mapIndex) ) {
            ++task.retryCount;
            if ( task.retryCount > 3 ) {
                LOG.fatal("unable to retreive reactive mapoutput "+mapId);
            } else {
                LOG.info(mapId + " is precommitted. retry "+mapId+" ("+task.retryCount+")");
                abort = false;
                putBackKnownReactiveMapOutput(task);
            }
        }
        
        if ( abort ) {
            task.abort();
        }
//        scheduledReactiveMaps.remove(mapId.getTaskID());
        workingReactiveMaps.remove(mapId.getTaskID());

        // statistics?
        checkReducerHealth();
        failedShuffleCounter.increment(1);
        notifyAll();
    }

    /**
     * commit reactive map output to make a room for the remaining outputs
     * @throws IOException 
     */
    public synchronized void commitPendingReactiveOutput() throws IOException {
        for ( ReactiveMapOutput<K,V> mapout : workingReactiveMaps ) {
            int mapId = mapout.getTaskID().getId();
            int committed = mapout.precommit();
            reactiveInProgress.set(mapId); // now this will no longer available for fetch
            
            LOG.info("precommitting "+committed+" reactive map outputs of "+mapout.getTaskAttemptID(mapId));
        }
    }

    public synchronized void updateTaskState(Internal[] takeovers) {
        for ( Internal event : takeovers ) {
            int mapIndex = event.getPartition();
            TaskState state = event.getState();
            TaskState old = takeoverState[mapIndex];
            
            takeoverState[mapIndex] = state;
            
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("map "+mapIndex+" state "+old+" -> "+state);
            }

            switch ( state ) {
            case TAKEOVER:
                {
                    // if we have received take over, readjust finished map
                    boolean reallyFinished = orgMapOutput.get(mapIndex) && reactiveMapOutput.get(mapIndex);
                    if ( finishedMaps[mapIndex] && !reallyFinished ) {
                        ++remainingMaps;
                    }
                }
                break;
            case COMPLETE:
                {
                    // on receiving complete, check whether the original output is already received.
                    // if so, set finishedMaps ...
                    if ( orgMapOutput.get(mapIndex) && ! finishedMaps[mapIndex] ) {
                        finishedMaps[mapIndex] = true;
                        if ( --remainingMaps == 0 ) {
                            notifyAll();
                        }
                        
                        int mapsDone = totalMaps - remainingMaps;
                        progress.set((float) mapsDone / totalMaps);
                        
                        // update the status
                        float mbs = (float) totalBytesShuffledTillNow / (1024 * 1024);
                        long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;
            
                        float transferRate = mbs / secsSinceStart;
                        String statusString = mapsDone + " / " + totalMaps + " copied.";
                        status.setStateString(statusString);
                        progress.setStatus("copy(" + mapsDone + " of " + totalMaps + " at " + mbpsFormat.format(transferRate) + " MB/s)");
            
                        LOG.info("map " + mapIndex + " done by original map " + statusString);

                    }
                }
                break;
            case CANCEL_TAKEOVER:
                {
                    // if we have received cancel take over, readjust finished map
                    boolean reallyFinished = orgMapOutput.get(mapIndex) || reactiveMapOutput.get(mapIndex);
                    if ( ! finishedMaps[mapIndex] && reallyFinished ) {
                        finishedMaps[mapIndex] = true;
                        if ( --remainingMaps == 0 ) {
                            notifyAll();
                        }
                        
                        int mapsDone = totalMaps - remainingMaps;
                        progress.set((float) mapsDone / totalMaps);
                    }
                }
                break;
            case PREPARE_TAKEOVER:
                {
                    if ( finishedMaps[mapIndex] ) {
                        finishedMaps[mapIndex] = false;
                        ++remainingMaps;
                    }
                }
                break;
            default:
                break;
            }
        }
    }
}
