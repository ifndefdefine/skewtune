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

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.SkewTuneJobConfig;
import skewtune.mapreduce.lib.input.RecordLength;

/** An {@link RecordReader} for {@link SequenceFile}s. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFileRecordReader<K, V> implements SplittableRecordReader<K, V>, ScannableReader<K,V> {
  private static final Log LOG = LogFactory.getLog(SequenceFileRecordReader.class.getName());

  private SequenceFile.Reader in;
  private long start;
  private long end;
  private boolean more = true;
  protected Configuration conf;
  
  // SKEWREDUCE
  // monitoring skew
  private long pos; // current position
  private long len;
  private float minFactor;
  private FileSplit split;
  private boolean stopped;
  
  private long lastSyncPos;
  private int  recSinceSync;
 
  public SequenceFileRecordReader(Configuration conf, FileSplit split)
    throws IOException {
    Path path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    this.in = new SequenceFile.Reader(fs, path, conf);
    this.end = split.getStart() + split.getLength();
    this.conf = conf;
    
    RelativeOffset ro = new RelativeOffset(conf);

    if ( ro.shouldOverride(conf,split.getStart()) ) {
        if ( LOG.isDebugEnabled() ) {
            LOG.debug("override starting offset using relative offset = "+ro);
        }
        if ( ro.getOffset() > in.getPosition() ) {
            in.sync(ro.getOffset());
            // now skip over
            more = skipRecs(ro.getRecOffset());
        }
    } else {
        if (split.getStart() > in.getPosition())
            in.sync(split.getStart());                  // sync to start
    }

    this.start = in.getPosition();
    more = start < end;
    
    this.len = split.getLength();
    minFactor = conf.getFloat(SkewTuneJobConfig.MAP_NUM_SPLITS_ATTR, 0.0f); // by default, never report to tracker
    
    this.split = split;
    
    if ( LOG.isDebugEnabled() ) {
        LOG.debug("start to reading input stream at "+this.start);
    }
  }
  
  private boolean skipRecs(int n) throws IOException {
      DataOutputBuffer rawKey = new DataOutputBuffer();
      ValueBytes rawVal = in.createValueBytes();
      for ( int i = 0; i <= n; ++i ) {
          if ( in.nextRaw(rawKey, rawVal) < 0 ) return false;
      }
      return true;
  }


  /** The class of key that must be passed to {@link
   * #next(Object, Object)}.. */
  public Class getKeyClass() { return in.getKeyClass(); }

  /** The class of value that must be passed to {@link
   * #next(Object, Object)}.. */
  public Class getValueClass() { return in.getValueClass(); }
  
  @SuppressWarnings("unchecked")
  public K createKey() {
    return (K) ReflectionUtils.newInstance(getKeyClass(), conf);
  }
  
  @SuppressWarnings("unchecked")
  public V createValue() {
    return (V) ReflectionUtils.newInstance(getValueClass(), conf);
  }
    
  public synchronized boolean next(K key, V value) throws IOException {
    if (!more) return false;
    pos = in.getPosition();
    boolean remaining = (in.next(key) != null);
    if (remaining) {
      getCurrentValue(value);
    }
    if ( (pos >= end || stopped) && in.syncSeen()) {
      more = false;
    } else {
      more = remaining;
    }
    
    if ( more ) {
        if ( in.syncSeen() ) {
            lastSyncPos = pos;
            recSinceSync = 0;
        } else {
            ++recSinceSync;
        }
    }
    
    return more;
  }
  
  protected synchronized boolean next(K key)
    throws IOException {
    if (!more) return false;
    pos = in.getPosition();
    boolean remaining = (in.next(key) != null);
    if ( (pos >= end || stopped) && in.syncSeen()) {
      more = false;
    } else {
      more = remaining;
    }
    
    if ( more ) {
        if ( in.syncSeen() ) {
            lastSyncPos = pos;
            recSinceSync = 0;
        } else {
            ++recSinceSync;
        }
    }

    return more;
  }
  
  protected synchronized void getCurrentValue(V value)
    throws IOException {
    in.getCurrentValue(value);
    
    // check record size
    long recEnd = in.getPosition();
    int recLen = (int)(recEnd - pos);
    float factor = len / (float)recLen;
    if ( factor < minFactor ) {
        minFactor = factor;
        SkewNotifier.registerNotification(minFactor, pos, recLen);
    }
  }
  
  /**
   * Return the progress within the input split
   * @return 0.0 to 1.0 of the input byte range
   */
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
    }
  }
  
  public synchronized long getPos() throws IOException {
    return in.getPosition();
  }
  
  protected synchronized void seek(long pos) throws IOException {
    in.seek(pos);
  }
  public synchronized void close() throws IOException {
      if ( LOG.isDebugEnabled() ) {
          LOG.debug("closing input stream at "+in.getPosition());
      }
      in.close();
  }

/*
    @Override
    public synchronized List<InputSplit> split(int n, boolean cont) throws IOException {
        long pos = in.getPosition();

        long remain = split.getLength() - (pos+1);
        long perSplit = (long)Math.ceil(remain / (double) ( cont ? n+1 : n ));
        
        long startPos = pos+1;
        end = in.getPosition();
        
        if ( cont ) {
            startPos += perSplit;
            end += perSplit;
            remain -= perSplit;
        }
        
        List<InputSplit> splits = new ArrayList<InputSplit>(n);
        for ( int i = 0; i < n; ++i ) {
            long sz = Math.min(remain, perSplit);
            FileSplit fileSplit = new FileSplit(split.getPath(), startPos, sz, (String[])null);
            splits.add(fileSplit);
            startPos += sz;
            remain -= sz;
        }
        
        return splits;
    }
*/

    /**
     * @param out contains file name, current offset, and supposed length
     */
    @Override
    public synchronized StopStatus tellAndStop(DataOutput out) throws IOException {
        if ( ! more  || stopped || pos >= end ) {
            if ( LOG.isInfoEnabled() ) {
                LOG.info("tellAndStop(): already stopped pos="+pos+"; end="+end);
            }
            return StopStatus.CANNOT_STOP;
        }
        
        Text.writeString(out, split.getPath().toString());
        out.writeLong(in.getPosition());
        out.writeLong(split.getStart()+split.getLength());
        
        if ( LOG.isDebugEnabled() ) {
            LOG.debug("tellAndStop(): stop reading input stream at position "+in.getPosition());
        }
        
        stopped = true;
        
        return StopStatus.STOPPED;
    }

    /*
    @Override
    public synchronized boolean scanNextKeyValue(LongWritable offset, RecordLength recLen) throws IOException {
        if ( ! more ) return false;
        
        if ( rawScanKey == null ) rawScanKey = new DataOutputBuffer(65536);
        if ( rawScanVal == null ) rawScanVal = in.createValueBytes();
        
        pos = in.getPosition();
        int len = in.nextRaw(rawScanKey, rawScanVal);
        long endOff = in.getPosition();
        
        boolean remaining = len >= 0;
        boolean syncSeen = in.syncSeen();
        if ( (pos >= end) && syncSeen ) {
            more = false;
        } else {
            more = remaining;
        }
        
        offset.set(pos);
        recLen.set(rawScanKey.getLength(), rawScanVal.getSize(), (int)(endOff-pos), syncSeen);
        
        return more;
    }
    */
    
    // we should buffer one record because of the sync mark.
    // if syncseen is true, that means the previous record is the boundary -- end of one chunk, not the current one.

    private DataOutputBuffer rawScanKey;
    private ValueBytes rawScanVal;
    
    // buffer
    private long prevPos;
    private RecordLength prevRecLen;
    
    @Override
    public synchronized void initScan() throws IOException {
        rawScanKey = new DataOutputBuffer(65536);
        rawScanVal = in.createValueBytes();
        prevRecLen = new RecordLength();
        
        pos = in.getPosition();
        int len = in.nextRaw(rawScanKey, rawScanVal);
        long endOff = in.getPosition();

        boolean remaining = len >= 0;
        boolean syncSeen = in.syncSeen();
        if ( (pos >= end) && syncSeen ) {
            more = false;
        } else {
            more = remaining;
        }
        
        if ( more ) {
            prevPos = pos;
            prevRecLen.set(rawScanKey.getLength(), rawScanVal.getSize(), (int)(endOff-pos), false); // don't care about sync
        } else {
            prevPos = -1;
        }
    }

    @Override
    public synchronized boolean scanNextKeyValue(LongWritable offset, RecordLength recLen) throws IOException {
        if ( ! more ) {
            if ( prevPos < 0 ) {
                return false;
            } else {
                // last record. must be splittable
                offset.set(prevPos);
                recLen.set(prevRecLen.getKeyLength(), prevRecLen.getValueLength(), prevRecLen.getRecordLength(), true);
                prevPos = -1;
                return true;
            }
        }
        
        rawScanKey.reset();
        pos = in.getPosition();
        int len = in.nextRaw(rawScanKey, rawScanVal);
        long endOff = in.getPosition();
        
        boolean remaining = len >= 0;
        boolean syncSeen = in.syncSeen();
        if ( (pos >= end) && syncSeen ) {
            more = false;
        } else {
            more = remaining;
        }
        
        // copy buffered entry
        offset.set(prevPos);
        recLen.set(prevRecLen.getKeyLength(), prevRecLen.getValueLength(), prevRecLen.getRecordLength(), syncSeen);
        
        if ( more ) {
            prevPos = pos;
            prevRecLen.set(rawScanKey.getLength(), rawScanVal.getSize(), (int)(endOff-pos), false);
        } else {
            prevPos = -1; // no more buffered
        }
        
        return true;
    }

    @Override
    public synchronized void closeScan() throws IOException {
        rawScanKey = null;
        rawScanVal = null;
        prevRecLen = null;
    }

    @Override
    public RelativeOffset getRelativeOffset() {
        return new RelativeOffset(lastSyncPos,recSinceSync);
    }
}
