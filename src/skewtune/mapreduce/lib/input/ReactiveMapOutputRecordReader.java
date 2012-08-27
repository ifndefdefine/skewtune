package skewtune.mapreduce.lib.input;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.IFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputIndex;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.SkewTuneJobConfig;
import skewtune.mapreduce.lib.input.MapOutputSplit.ReactiveOutputSplit;
import skewtune.mapreduce.lib.partition.GroupComparator;
import skewtune.utils.Base64;

public class ReactiveMapOutputRecordReader<K,V> extends RecordReader<K,V>
implements MRJobConfig, SkewTuneJobConfig, org.apache.hadoop.mapred.RecordReader<K,V> {
    private static final Log LOG = LogFactory.getLog(ReactiveMapOutputRecordReader.class);

    protected Configuration conf;
    private Counter inputByteCounter;
    private Counter skippedCounter;

    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;
    
    Class<?> keyClass;
    Class<?> valClass;
    
    private K key;
    private V value;
    
    private DataInputBuffer buffer = new DataInputBuffer();
    
    private List<ReactiveOutputSplit> splits;
    private int currentSplit;
    private TaskID taskid;

    // Decompression of map-outputs
    private CompressionCodec codec;
    private Path orgOutPathDir;
//    private IFile.Reader<K, V> reader;
    private SkippingIFileReader<K,V> reader;
    
    private long totalBytesReadSoFar;
    private long totalBytes;
    
    /**
     * read keys strictly greater than minKey
     */
    private byte[] minKey;
    private byte[] maxKey;
    private RawComparator<K> comp;
    private boolean useRawComp;
    private K minKeyObj;


    @SuppressWarnings("unchecked")
    @Override
    public void initialize(InputSplit split,TaskAttemptContext context) throws IOException, InterruptedException {
        MapContext<K, V, ?, ?> mapContext = (MapContext<K, V, ?, ?>)context;
        orgOutPathDir = org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.getOutputPath(context).getParent();
        initialize(mapContext.getConfiguration(),
                mapContext.getMapOutputKeyClass(),
                mapContext.getMapOutputValueClass(),
                mapContext.getCounter(FileInputFormat.COUNTER_GROUP,FileInputFormat.BYTES_READ),
                mapContext.getCounter("SkewTune", "SkippedMapOutputRecs"),
                split,
                getComparator(mapContext));
    }
    
    @SuppressWarnings("unchecked")
    public void initialize(org.apache.hadoop.mapred.InputSplit split, JobConf job,Reporter reporter) throws IOException, InterruptedException {
        orgOutPathDir = org.apache.hadoop.mapred.FileOutputFormat.getOutputPath(job).getParent();
        initialize(job,
                job.getMapOutputKeyClass(),
                job.getMapOutputValueClass(),
                reporter.getCounter(FileInputFormat.COUNTER_GROUP, FileInputFormat.BYTES_READ),
                reporter.getCounter("SkewTune", "SkippedMapOutputRecs"),
                split,
                getComparator(job));
    }
    
    private RawComparator<K> getComparator(JobConf conf) {
        return GroupComparator.getInstance(conf);
    }
    private RawComparator<K> getComparator(MapContext context) {
        return GroupComparator.getInstance(context);
    }

    
    private void initialize(Configuration conf,Class<?> keyClass,Class<?> valClass,Counter counter,Counter skippedRecs,Object split,RawComparator<K> myComp)
    throws IOException, InterruptedException {
        this.conf = conf;
        this.inputByteCounter = counter;
        this.skippedCounter = skippedRecs;
        this.keyClass = keyClass;
        this.valClass = valClass;
        
        if ( inputByteCounter == null )
            throw new IllegalStateException("input byte counter is null!");
        
        // initialize compression codec
        if ( conf.getBoolean(MAP_OUTPUT_COMPRESS, false) ) {
            Class<? extends CompressionCodec> codecClass = conf.getClass(MAP_OUTPUT_COMPRESS_CODEC, DefaultCodec.class, CompressionCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
//            decompressor = CodecPool.getDecompressor(codec);
        } else {
            codec = null;
//            decompressor = null;
        }
        
        
        // FIXME should be network or file?
        taskid = TaskID.forName(conf.get(ORIGINAL_TASK_ID_ATTR));
        
        if ( LOG.isInfoEnabled() ) {
            LOG.info("Original task id = "+taskid+"; original output path = "+orgOutPathDir);
        }

        splits = new ArrayList<ReactiveOutputSplit>();
        if ( split instanceof MapOutputSplit ) {
            splits.addAll(((MapOutputSplit)split).getReactiveOutputs());
        } else if ( split instanceof CombinedMapOutputSplit ) {
            for ( MapOutputSplit s : (CombinedMapOutputSplit)split ) {
                splits.addAll(((MapOutputSplit)s).getReactiveOutputs());
            }
        } else {
            throw new IllegalArgumentException("Unsupported input split type: "+split.getClass());
        }
        
        if ( LOG.isDebugEnabled() ) {
            for ( int i = 0; i < splits.size(); ++i ) {
                LOG.debug("input split "+i+": "+splits.get(i));
            }
        }
        
        for ( ReactiveOutputSplit s : splits ) {
            totalBytes += s.getLength();
        }

        SerializationFactory factory = new SerializationFactory(conf);
        keyDeserializer = (Deserializer<K>)factory.getDeserializer(keyClass);
        valueDeserializer = (Deserializer<V>)factory.getDeserializer(valClass);

        keyDeserializer.open(buffer);
        valueDeserializer.open(buffer);
        
        String keyStr = conf.get(REACTIVE_REDUCE_MIN_KEY);
        if ( keyStr != null && keyStr.length() > 0 ) {
            // set minKey
            minKey = Base64.decode(keyStr);
            buffer.reset(minKey, minKey.length);
            minKeyObj = keyDeserializer.deserialize(null);
        }
        
        keyStr = conf.get(REACTIVE_REDUCE_MAX_KEY);
        if ( keyStr != null && keyStr.length() > 0 ) {
            maxKey = Base64.decode(keyStr);
        }
        if ( minKey != null || maxKey != null ) {
            this.comp = myComp;
            useRawComp = ! (myComp instanceof GroupComparator); // we got sort comparator impl.
        }
    }
    
    private boolean nextRawKey() throws IOException {
        while ( reader == null || ! reader.nextRawKey(buffer) ) {
            if ( ! this.initializeRecordReader() ) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
//        boolean hasNextKey;
//        while ( (hasNextKey = nextRawKey()) ) {
//            if ( comp == null
//                    || comp.compare(minKey, 0, minKey.length,
//                            buffer.getData(), 0, buffer.getLength()) < 0 ) {
//                break;
//            } else {
//                // otherwise, skip this key
//                reader.nextRawValue(buffer);
//                this.skippedCounter.increment(1);
//            }
//        }
//        if ( ! hasNextKey ) {
//            return false;
//        }
//        key = keyDeserializer.deserialize(key);
//        reader.nextRawValue(buffer);
//        value = valueDeserializer.deserialize(value);
//        return true;
        return next(key,value);
    }

    @Override
    public K getCurrentKey() {
        return key;
    }

    @Override
    public V getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        long subprogress = 0;    // bytes processed in current split
        if (null != reader) {
          // idx is always one past the current subsplit's true index.
          subprogress = reader.getVirtualReadBytes();
        }
        return Math.min(1.0f,  (totalBytesReadSoFar + subprogress)/(float)totalBytes);
    }

    @Override
    public synchronized void close() throws IOException {
        if ( reader != null ) {
            this.reader.close();
            reader = null;
        }
    }

    @Override
    public boolean next(K key, V value) throws IOException {
        boolean hasNextKey;
        while ( (hasNextKey = nextRawKey()) ) {
            /*
            if ( comp == null
                    || comp.compare(minKey, 0, minKey.length,
                            buffer.getData(), 0, buffer.getLength()) < 0 ) {
                break;
            } else {
                // otherwise, skip this key
                reader.nextRawValue(buffer);
                this.skippedCounter.increment(1);
            }
            */
            if ( comp == null ) {
                this.key = keyDeserializer.deserialize(key);
                break;
            }
            
            // check whether we should use raw comparator or group comparator
            if ( useRawComp ) {
                if ( comp.compare(minKey, 0, minKey.length, buffer.getData(), 0, buffer.getLength()) < 0 ) {
                    this.key = keyDeserializer.deserialize(key);
                    break;
                }
            } else {
                // we need to compare it against group comparator
                this.key = keyDeserializer.deserialize(key);
                if ( comp.compare(minKeyObj, key) < 0 ) {
                    break;
                }
            }

            // otherwise, skip this key
            reader.nextRawValue(buffer);
            this.skippedCounter.increment(1);            
        }
        if ( ! hasNextKey ) {
            return false;
        }
//        this.key = keyDeserializer.deserialize(key);
        reader.nextRawValue(buffer);
        this.value = valueDeserializer.deserialize(value);
        return true;
    }

    @Override
    public K createKey() {
        key = (K) ReflectionUtils.newInstance(keyClass, conf);
        return key;
    }

    @Override
    public V createValue() {
        value = (V) ReflectionUtils.newInstance(valClass, conf);
        return value;
    }

    @Override
    public long getPos() throws IOException {
        return totalBytesReadSoFar + (( reader == null ) ? 0 : reader.getVirtualReadBytes());
    }
    
    private boolean initializeRecordReader() throws IOException {
        if ( currentSplit == splits.size() ) {
            return false;
        }
        
        if ( reader != null ) {
            totalBytesReadSoFar += reader.bytesRead;
            reader.close();
        }
        
        ReactiveOutputSplit split = splits.get(currentSplit);
        
        Path path = split.getPath(orgOutPathDir);
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream hdfsInput = fs.open(path,4096*1024); // 4MB buffer?
        hdfsInput.seek(split.getCompressedOffset());
        
        if ( LOG.isInfoEnabled() ) {
            LOG.info("reading "+path);
            LOG.info(split);
        }
        
        MapOutputIndex.Record mapOut = split.getOutputIndex();
        
        reader = new SkippingIFileReader<K,V>(conf,hdfsInput,mapOut.getPartLength(),codec,null);
        long skipped = reader.skip(split.getOffset(),split.getLength());
        if ( skipped > 0 ) {
            LOG.info("skipped "+skipped+" bytes");
        }
        
        ++currentSplit;
        
        return true;
    }
    
    static class SkippingIFileReader<K,V> extends IFile.Reader<K, V> {
        private long offset;
        private long endOfRead;

        public SkippingIFileReader(Configuration conf, FSDataInputStream in,
                long length, CompressionCodec codec,
                org.apache.hadoop.mapred.Counters.Counter counter)
                throws IOException {
            super(conf, in, length, codec, counter);
            // FIXME Oh, well... just for now... we can use more efficient data exchange format
            disableChecksumValidation();
        }
        
        /**
         * this is gross but the underlying stream does not support skipping so we simply start from beginning and throw away the data until we hit the first record after the offset.
         * @param off
         * @param len
         * @return
         * @throws IOException
         */
        public long skip(long off,long len) throws IOException {
            this.offset = off;
            this.endOfRead = offset + len;
            
            byte[] buffer = new byte[1024*1024]; // read 1 MB at a time
            
            // now locate the position
            while ( bytesRead < offset ) {
                if ( ! positionToNextRecord(dataIn) ) break;
                int remain = currentKeyLength + currentValueLength;
                
                if ( remain + bytesRead > endOfRead ) {
                    // yay! nothing to read!!
                    bytesRead += remain; // following nextRawKey should fail!
                    return bytesRead;
                }
                
                while (remain > 0 ) {
                    int i = dataIn.read(buffer,0,Math.min(remain,buffer.length)); // we are discarding the data
                    if ( i < 0 ) {
                        throw new IOException("Unexpected EOF! Still expecting "+remain+" bytes");
                    }
                    remain -= i;
                    bytesRead += i;
                }
            }
            
            // now we passed the offset. good to move on next record.
            return bytesRead;
        }
        
        @Override
        public boolean nextRawKey(DataInputBuffer key) throws IOException {
            if ( bytesRead > endOfRead )
                return false;
            return super.nextRawKey(key);
        }
        
        public long getVirtualReadBytes() {
            return Math.max(0,bytesRead - offset);
        }
    }
}
