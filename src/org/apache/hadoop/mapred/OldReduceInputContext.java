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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.TaskProgress.ReduceProgress;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

import skewtune.mapreduce.SampleKeyPool;
import skewtune.mapreduce.SkewTuneJobConfig;
import skewtune.mapreduce.PartitionMapOutput.MapOutputPartition;
import skewtune.mapreduce.SampleKeyPool.KeyInfo;
import skewtune.mapreduce.SampleKeyPool.KeyInterval;
import skewtune.mapreduce.SampleKeyPool.OutputContext;
import skewtune.mapreduce.SampleMapOutput.KeyPair;

/**
 * The context passed to the {@link Reducer}.
 *
 * copied from ReduceContextImpl.
 *
 * on stop, we first mark current position and buffer all values
 * in the current key group.
 *
 * if we have no more data, return can't split.
 *
 * otherwise,
 *   reset() -- this will resume the computing thread
 *   spawn scan thread, which continues to scan and collect information
 *
 * at the end,
 *   call join() on scan thread to complete
 *
 * @param <KEYIN> the class of the input keys
 * @param <VALUEIN> the class of the input values
 * @param <KEYOUT> the class of the output keys
 * @param <VALUEOUT> the class of the output values
 */
public class OldReduceInputContext<KEYIN,VALUEIN> implements Iterator<VALUEIN>, Iterable<VALUEIN>, SkewTuneJobConfig {
  private static final Log LOG = LogFactory.getLog(OldReduceInputContext.class);

  /**
   * assumed disk bandwidth. 30MB/s
   */
//  public static final double DISK_BW = (double)(30 << 20);
//  public static final double LOCALSCAN_THRESHOLD = 60.;
  private long diskBw;
  private float maxScanTime;
  
  private RawKeyValueIterator input;
  private Counter inputValueCounter;
  private Counter inputKeyCounter;
  private RawComparator<KEYIN> comparator;
  private KEYIN key;                                  // current key
  private VALUEIN value;                              // current value
  private boolean firstValue = false;                 // first value in key
  private boolean nextKeyIsSame = false;              // more w/ this key
  private boolean hasMore;                            // more in file
  protected StatusReporter reporter;
  private Deserializer<KEYIN> keyDeserializer;
  private Deserializer<VALUEIN> valueDeserializer;
  private DataInputBuffer buffer = new DataInputBuffer();
  private BytesWritable currentRawKey = new BytesWritable();
//  private ValueIterable iterable = new ValueIterable();
  private boolean isMarked = false;
  private BackupStore<KEYIN,VALUEIN> backupStore;
  private final SerializationFactory serializationFactory;
  private final Class<KEYIN> keyClass;
  private final Class<VALUEIN> valueClass;
  private final Configuration conf;
  private final TaskAttemptID taskid;
  private int currentKeyLength = -1;
  private int currentValueLength = -1;

  private ReduceProgress progress;
  private volatile ScanTask scanThread;
  private volatile boolean stopped;
  private volatile int buffered;
  private boolean forceLocalScan;
  
  private org.apache.hadoop.mapreduce.Counter scanByteCounter;
  private org.apache.hadoop.mapreduce.Counter scanRecordCounter;
  private org.apache.hadoop.mapreduce.Counter scanGroupCounter;
  private org.apache.hadoop.mapreduce.Counter scanMillisCounter;
  
  public OldReduceInputContext(Configuration conf, TaskAttemptID taskid,
                           RawKeyValueIterator input, 
                           Counter inputKeyCounter,
                           Counter inputValueCounter,
                           StatusReporter reporter,
                           RawComparator<KEYIN> comparator,
                           Class<KEYIN> keyClass,
                           Class<VALUEIN> valueClass,
                           ReduceProgress progress
                          ) throws InterruptedException, IOException {
    //super(conf, taskid, output, committer, reporter);
    this.input = input;
    this.inputKeyCounter = inputKeyCounter;
    this.inputValueCounter = inputValueCounter;
    this.comparator = comparator;
    this.serializationFactory = new SerializationFactory(conf);
    this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
    this.keyDeserializer.open(buffer);
    this.valueDeserializer = serializationFactory.getDeserializer(valueClass);
    this.valueDeserializer.open(buffer);
    hasMore = input.next();
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.conf = conf;
    this.taskid = taskid;
    this.progress = progress;
    this.reporter = reporter;
    
    scanByteCounter = reporter.getCounter("SkewReduce", "SCAN_BYTES");
    scanRecordCounter = reporter.getCounter("SkewReduce", "SCAN_RECORDS");
    scanGroupCounter = reporter.getCounter("SkewReduce", "SCAN_GROUPS");
    scanMillisCounter = reporter.getCounter("SkewReduce", "SCAN_MILLIS");
    
    diskBw = conf.getLong(SKEWTUNE_REPARTITION_DISK_BW, 30 << 20);
    maxScanTime = conf.getFloat(SKEWTUNE_REPARTITION_MAX_SCAN_TIME, 60.f);
    forceLocalScan = conf.getBoolean(SKEWTUNE_REPARTITION_LOCALSCAN_FORCE,false);
  }

  /** Start processing next unique key. */
  public boolean nextKey() throws IOException,InterruptedException {
    return nextKey(false);
  }
  
  private boolean useIterator;

  private synchronized boolean nextKey(boolean scan)
  throws IOException,InterruptedException {
    boolean ret = false;
    
    if ( stopped && ! scan) {
        // double check the backup store.
//        hasBackup = ! hasBackup && backupStore != null && backupStore.hasNext();
//        return hasBackup; // there's something to consume
        return false;
    }

    while (hasMore && nextKeyIsSame) {
      nextKeyValue(scan);
    }
    
    if (hasMore) {
      if (inputKeyCounter != null && !scan ) {
        inputKeyCounter.increment(1);
      }
      ret = nextKeyValue(scan);
    }
    
    if ( ret && !scan ) {
        useIterator = true; // the compute thread will get the new iterator
    }
    
    return ret;
  }

  public boolean nextKeyValue()
  throws IOException, InterruptedException {
    return nextKeyValue(false);
  }

  private synchronized boolean nextKeyValue(boolean scan)
  throws IOException, InterruptedException {
    if (!hasMore) {
      key = null;
      value = null;
      return false;
    }
    firstValue = !nextKeyIsSame;
    DataInputBuffer nextKey = input.getKey();
    currentRawKey.set(nextKey.getData(), nextKey.getPosition(), 
                      nextKey.getLength() - nextKey.getPosition());
    if ( ! scan ) {
        buffer.reset(currentRawKey.getBytes(), 0, currentRawKey.getLength());
        key = keyDeserializer.deserialize(key);
    }
    DataInputBuffer nextVal = input.getValue();
    if ( ! scan ) {
        buffer.reset(nextVal.getData(), nextVal.getPosition(), nextVal.getLength());
        value = valueDeserializer.deserialize(value);
    }

    currentKeyLength = nextKey.getLength() - nextKey.getPosition();
    currentValueLength = nextVal.getLength() - nextVal.getPosition();

    if ( ! stopped && isMarked ) {
      backupStore.write(nextKey, nextVal);
    }

    hasMore = input.next();
    if (hasMore) {
      nextKey = input.getKey();
      nextKeyIsSame = comparator.compare(currentRawKey.getBytes(), 0, 
                                     currentRawKey.getLength(),
                                     nextKey.getData(),
                                     nextKey.getPosition(),
                                     nextKey.getLength() - nextKey.getPosition()
                                         ) == 0;
    } else {
      nextKeyIsSame = false;
    }
    
    if ( !scan )
        inputValueCounter.increment(1);
    
    return true;
  }

  public KEYIN getCurrentKey() {
    return key;
  }

  public VALUEIN getCurrentValue() {
    return value;
  }
  
  public synchronized Future<StopStatus> stop(StopContext context)
  throws IOException, InterruptedException {
    // FIXME if we are going to immediately return, following is unnecessary
    // or, we can decide based on the number of bytes remaining
    Future<StopStatus> result;
//    ValueIterator i = (ValueIterator)(iterable.iterator());
//    boolean isstopped = i.stop();
    long beforeInputValues = this.inputValueCounter.getValue();
    boolean isstopped = stopIterator();
    if ( isstopped ) {
//        double speed = progress.getSortSpeed();
        long   bytesRemain = progress.getRemainingBytes(null);
//        double estScanTime = bytesRemain / speed;
        double estScanTime = bytesRemain / diskBw;

        DataOutputBuffer buffer = context.getBuffer();
        
        // FIXME estimated scan time is less than a minute
        boolean localScan = forceLocalScan || estScanTime < maxScanTime;
//        synchronized (this) {
            WritableUtils.writeVInt(buffer, currentRawKey.getLength()); // min key
            buffer.write(currentRawKey.getBytes(),0,currentRawKey.getLength());
//        }
            
        context.setRemainBytes(bytesRemain);
        
        if ( localScan ) {
            result = context.execute(new ScanTask(context));
        } else {
            buffer.writeInt(0);
            result = StopStatus.future(context.setStatus(StopStatus.STOPPED));
        }
        LOG.info("bytes remain="+bytesRemain+";est. scan time = "+estScanTime+"; localScan = "+localScan+"; before="+beforeInputValues+"; buffered="+buffered+"; total="+(beforeInputValues+buffered));
    } else {
        result = StopStatus.future(context.setStatus(StopStatus.CANNOT_STOP));
        LOG.info("cannot stop! before="+beforeInputValues+"; buffered="+buffered+"; total="+(beforeInputValues+buffered));
    }
    
    return result;
  }

  /**
   * directly called from reduce task.
   * @param context
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  public StopStatus scan(StopContext context) throws Exception {
    DataOutputBuffer buffer = context.getBuffer();
    WritableUtils.writeVInt(buffer, 0); // min key
    buffer.write(currentRawKey.getBytes(),0,0);
    return new ScanTask(context).call();
  }
  
  /// ITERATOR IMPLEMENTATION
  
//  protected class ValueIterator implements Iterator<VALUEIN> {

    private boolean inReset = false;
    private boolean clearMarkFlag = false;
    
    @Override
    public synchronized boolean hasNext() {
      try {
        if (inReset && backupStore.hasNext()) {
          return true;
        } 
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("hasNext failed", e);
      }
      return ! stopped && (firstValue || nextKeyIsSame);
    }

    /**
     * stop iteration.
     * @throws IOException 
     */
    private boolean stopIterator() throws IOException {
      mark();
      while ( hasNext() ) {
          // if this is the first record, we don't need to advance
          if (firstValue) {
            firstValue = false;
          }
          // if this isn't the first record and the next key is different, they
          // can't advance it here.
          if (!nextKeyIsSame) {
            throw new NoSuchElementException("iterate past last value");
          }
          // otherwise, go to the next key/value pair
          try {
            nextKeyValue(true);
          } catch (IOException ie) {
            throw new RuntimeException("next value iterator failed", ie);
          } catch (InterruptedException ie) {
            // this is bad, but we can't modify the exception list of java.util
            throw new RuntimeException("next value iterator interrupted", ie);
          }
          ++buffered;
      }
      if ( hasMore ) {
          stopped = true;
      }
      reset();
      return stopped;
    }

    @Override
    public synchronized VALUEIN next() {
      if (inReset) {
        try {
          if (backupStore.hasNext()) {
            backupStore.next();
            DataInputBuffer next = backupStore.nextValue();
            buffer.reset(next.getData(), next.getPosition(), next.getLength());
            value = valueDeserializer.deserialize(value);
            inputValueCounter.increment(1);
            return value;
          } else {
            inReset = false;
            backupStore.exitResetMode();
            if (clearMarkFlag) {
              clearMarkFlag = false;
              isMarked = false;
            }

            if ( stopped ) {
                throw new NoSuchElementException("iterate past last value");
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException("next value iterator failed", e);
        }
      } 

      // if this is the first record, we don't need to advance
      if (firstValue) {
        firstValue = false;
        return value;
      }
      // if this isn't the first record and the next key is different, they
      // can't advance it here.
      if (!nextKeyIsSame) {
        throw new NoSuchElementException("iterate past last value");
      }
      // otherwise, go to the next key/value pair
      try {
        nextKeyValue();
        return value;
      } catch (IOException ie) {
        throw new RuntimeException("next value iterator failed", ie);
      } catch (InterruptedException ie) {
        // this is bad, but we can't modify the exception list of java.util
        throw new RuntimeException("next value iterator interrupted", ie);        
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove not implemented");
    }

    private void mark() throws IOException {
      if (backupStore == null) {
        backupStore = new BackupStore<KEYIN,VALUEIN>(conf, taskid);
      }
      isMarked = true;
      if (!inReset) {
        backupStore.reinitialize();
        if (currentKeyLength == -1) {
          // The user has not called next() for this iterator yet, so
          // there is no current record to mark and copy to backup store.
          return;
        }
        assert (currentValueLength != -1);
        int requestedSize = currentKeyLength + currentValueLength + 
          WritableUtils.getVIntSize(currentKeyLength) +
          WritableUtils.getVIntSize(currentValueLength);
        DataOutputStream out = backupStore.getOutputStream(requestedSize);
        writeFirstKeyValueBytes(out);
        backupStore.updateCounters(requestedSize);
      } else {
        backupStore.mark();
      }
    }

    private void reset() throws IOException {
      // We reached the end of an iteration and user calls a 
      // reset, but a clearMark was called before, just throw
      // an exception
      if (clearMarkFlag) {
        clearMarkFlag = false;
        backupStore.clearMark();
        throw new IOException("Reset called without a previous mark");
      }
      
      if (!isMarked) {
        throw new IOException("Reset called without a previous mark");
      }
      inReset = true;
      backupStore.reset();
    }

    /**
     * This method is called to write the record that was most recently
     * served (before a call to the mark). Since the framework reads one
     * record in advance, to get this record, we serialize the current key
     * and value
     * @param out
     * @throws IOException
     */
    private void writeFirstKeyValueBytes(DataOutputStream out) 
    throws IOException {
      assert (getCurrentKey() != null && getCurrentValue() != null);
      WritableUtils.writeVInt(out, currentKeyLength);
      WritableUtils.writeVInt(out, currentValueLength);
      Serializer<KEYIN> keySerializer = 
        serializationFactory.getSerializer(keyClass);
      keySerializer.open(out);
      keySerializer.serialize(getCurrentKey());

      Serializer<VALUEIN> valueSerializer = 
        serializationFactory.getSerializer(valueClass);
      valueSerializer.open(out);
      valueSerializer.serialize(getCurrentValue());
    }
//  }

//  protected class ValueIterable implements Iterable<VALUEIN> {
//    private ValueIterator iterator = new ValueIterator();
//    @Override
//    public Iterator<VALUEIN> iterator() {
//      return iterator;
//    } 
//  }
    
    public Iterator<VALUEIN> iterator() {
        return this;
    }
  
  /**
   * Iterate through the values for the current key, reusing the same value 
   * object, which is stored in the context.
   * @return the series of values associated with the current key. All of the 
   * objects returned directly and indirectly from this method are reused.
   */
  public 
  Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
      return this;
  }
  

  protected class ScanValueIterator implements Iterator<VALUEIN> {
    @Override
    public synchronized boolean hasNext() {
      return firstValue || nextKeyIsSame;
    }

    @Override
    public synchronized VALUEIN next() {
      // if this is the first record, we don't need to advance
      if (firstValue) {
        firstValue = false;
        return value;
      }
      // if this isn't the first record and the next key is different, they
      // can't advance it here.
      if (!nextKeyIsSame) {
        throw new NoSuchElementException("iterate past last value");
      }
      // otherwise, go to the next key/value pair
      try {
        nextKeyValue(true);
        return value;
      } catch (IOException ie) {
        throw new RuntimeException("next value iterator failed", ie);
      } catch (InterruptedException ie) {
        // this is bad, but we can't modify the exception list of java.util
        throw new RuntimeException("next value iterator interrupted", ie);        
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove not implemented");
    }
  }

  class ScanTask implements OutputContext<byte[]>, Callable<StopStatus> {
      final StopContext context;
      final DataOutputBuffer output;
      SampleKeyPool<byte[]> pool;
      MapOutputPartition partition = new MapOutputPartition();
      int partitionCount;
      final long startAt;

      ScanTask(StopContext context) {
          this.context = context;
          // now we can determine min key and target interval
          pool = new SampleKeyPool<byte[]>(context.getNumMinSample(),context.getSampleInterval()); // # of min key = num reduce slots*10*factor, target interval
          output = context.getBuffer();
          startAt = System.currentTimeMillis();
      }


      @Override
      public StopStatus call() throws Exception {
          ScanValueIterator i = new ScanValueIterator();
          progress.startScan();
          
          try {
            while ( nextKey(true) ) {
              int nrecs = 0;
              long nbytes = 0;
              
              while ( i.hasNext() ) {
                  i.next();
                  ++nrecs;
                  nbytes += (WritableUtils.getVIntSize(currentKeyLength)
                          + WritableUtils.getVIntSize(currentValueLength)
                          + currentKeyLength+currentValueLength);
                  
              }
              
              scanRecordCounter.increment(nrecs);
              scanByteCounter.increment(nbytes);
              scanGroupCounter.increment(1);
              
              byte[] myKey = Arrays.copyOf(currentRawKey.getBytes(), currentRawKey.getLength());
              
              // if it passes limit, write it to the data output stream
              pool.addKeyGroup(this, myKey, nrecs, nbytes);
              
              reporter.progress();
            }
          } finally {
              progress.stopScan();
          }
          
        pool.reset(this);
        output.writeInt(partitionCount);
        
        long now = System.currentTimeMillis();
        
        if ( LOG.isInfoEnabled() ) {
            LOG.info("scanned keys = "+scanGroupCounter.getValue()
                    +"; records = "+scanRecordCounter.getValue()
                    +"; bytes = "+scanByteCounter.getValue()
                    +"; partitions = "+partitionCount
                    +"; elapsed = "+((now-startAt)*0.001)+"s");
        }
        
        scanMillisCounter.increment(now - startAt);
        
        // now, time to report back to tracker.
        // report current position or notify
        return context.setStatus(StopStatus.STOPPED);
    }

    @Override
    public void write(KeyInterval<byte[]> interval) throws IOException, InterruptedException {
        if ( interval.getLength() == 0 ) return;
        
        KeyInfo<byte[]> begin = interval.getBegin();
        KeyInfo<byte[]> end = interval.getEnd();

        partition.reset();

        if ( end.isSet() ) {
            partition.setMinKey(begin.getKey());
            partition.setMaxKey(end.getKey());
            partition.set(interval.getTotalRecords(), interval.getTotalBytes(), interval.getTotalKeys());
        } else {
            partition.setMinKey(begin.getKey());
            partition.setMaxKey(begin.getKey());
            partition.set(begin.getNumRecords(), begin.getTotalBytes(), 1);
        }
       
        if ( LOG.isTraceEnabled() ) {
            LOG.trace(partition);
        }
        
        partition.write(output);
        ++partitionCount;
    }
  }
}
