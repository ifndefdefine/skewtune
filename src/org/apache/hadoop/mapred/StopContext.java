package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

import skewtune.mapreduce.SkewTuneJobConfig;

public class StopContext implements Future<StopContext> {
    StopStatus status;
    final DataOutputBuffer output;
    ExecutorService execSvc;
    int numSlots;
    int sampleFactor;
    int minSamplePerSlot;
    volatile long remainBytes;
    volatile CountDownLatch  completion;
    
    public StopContext(Configuration conf) {
        status = StopStatus.STOPPING;
        output = new DataOutputBuffer(65536);
        numSlots = conf.getInt(SkewTuneJobConfig.SKEWTUNE_REPARTITION_SAMPLE_NUM_SLOTS, 20);
        sampleFactor = conf.getInt(SkewTuneJobConfig.SKEWTUNE_REPARTITION_SAMPLE_FACTOR,1);
        minSamplePerSlot = conf.getInt(SkewTuneJobConfig.SKEWTUNE_REPARTITION_MIN_SAMPLE_PER_SLOT, 10);
    }
    
    public void setRemainBytes(long bytes) {
        this.remainBytes = bytes;
    }
    
    public int getNumMinSample() {
        return minSamplePerSlot;
    }

    public int getSampleInterval() {
        return Math.max((int)Math.ceil((remainBytes / (double)(numSlots*minSamplePerSlot*sampleFactor))),1);
    }
    
    public synchronized void initialize() {
        execSvc = Executors.newSingleThreadExecutor();
        completion = new CountDownLatch(1);
    }
    
    public void emptyReduceResponse() throws IOException {
        WritableUtils.writeVInt(output, 0);
        output.writeInt(0);
    }
//    
//    public StopContext(StopStatus status) {
//        this.status = status;
//        output = null;
//        execSvc = null;
//    }
    
    public synchronized StopStatus setStatus(StopStatus status) {
        this.status = status;
        return status;
    }

    
    public synchronized StopStatus getStatus() { return status; }
    public DataOutputBuffer getBuffer() { return output; }
    
    public synchronized Future<StopStatus> execute(Callable<StopStatus> call) {
//        completion = new CountDownLatch(1);
        return execSvc.submit(call);
    }
    
    public boolean awaitTermination() throws InterruptedException {
        return awaitTermination(60);
    }
    
    public boolean awaitTermination(long to) throws InterruptedException {
        return completion == null ? true : completion.await(to, TimeUnit.SECONDS);
    }
    
    public void complete() {
        if ( completion != null )
            completion.countDown();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public StopContext get() throws InterruptedException, ExecutionException {
        return this;
    }

    @Override
    public StopContext get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return this;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }
}
