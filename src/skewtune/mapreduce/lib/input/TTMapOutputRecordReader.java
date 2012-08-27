package skewtune.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.TokenStorage;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.SkewTuneJobConfig;
import skewtune.mapreduce.lib.partition.GroupComparator;
import skewtune.utils.Base64;

public class TTMapOutputRecordReader<K,V> extends RecordReader<K,V>
implements MRJobConfig, SkewTuneJobConfig, org.apache.hadoop.mapred.RecordReader<K,V> {
    private static final Log LOG = LogFactory.getLog(TTMapOutputRecordReader.class);

    protected Configuration conf;
    private Counter inputByteCounter;
    private Counter skipCounter;

    private Deserializer<K> keyDeserializer;
    private Deserializer<V> valueDeserializer;
    
    Class<?> keyClass;
    Class<?> valClass;
    
    private K key;
    private V value;
    
    private MapOutputInputStreamReader<K,V> reader;
    private DataInputBuffer buffer = new DataInputBuffer();
    
    /**
     * read keys strictly greater than minKey
     */
    private byte[] minKey;
    private RawComparator<K> comp;
    private K minKeyObj;
    private boolean useRawComp;

    @SuppressWarnings("unchecked")
    @Override
    public void initialize(InputSplit split,TaskAttemptContext context) throws IOException, InterruptedException {
        MapContext<K, V, ?, ?> mapContext = (MapContext<K, V, ?, ?>)context;
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
    
    private void initialize(Configuration conf,Class<?> keyClass,Class<?> valClass,Counter counter,Counter skipCounter,Object split,
            RawComparator<K> myComp
            //,RawComparator<?> comp
            )
    throws IOException, InterruptedException {
        this.conf = conf;
        this.inputByteCounter = counter;
        this.skipCounter = skipCounter;
        this.keyClass = keyClass;
        this.valClass = valClass;
        
        if ( inputByteCounter == null )
            throw new IllegalStateException("input byte counter is null!");

        // FIXME should be network or file?
        TaskID taskid = TaskID.forName(conf.get(ORIGINAL_TASK_ID_ATTR));
        
        LOG.info("Original task id = "+taskid);

        TokenStorage ts = TokenCache.getTokenStorage();
        Token<JobTokenIdentifier> jt = (Token<JobTokenIdentifier>)ts.getToken(new Text(taskid.getJobID().toString()));
        SecretKey secretKey = JobTokenSecretManager.createSecretKey(jt.getPassword());
        
        if ( LOG.isDebugEnabled() ) {
            LOG.debug("original job password = "+Arrays.toString(jt.getPassword()));
            LOG.debug("original job secret = "+Arrays.toString(secretKey.getEncoded()));
        }

        ArrayList<MapOutputSplit> splits = new ArrayList<MapOutputSplit>();
        if ( split instanceof MapOutputSplit ) {
            splits.add((MapOutputSplit)split);
        } else if ( split instanceof CombinedMapOutputSplit ) {
            for ( MapOutputSplit s : (CombinedMapOutputSplit)split ) {
                splits.add(s);
            }
        } else {
            throw new IllegalArgumentException("Unsupported input split type: "+split.getClass());
        }
        
        if ( LOG.isDebugEnabled() ) {
            for ( int i = 0; i < splits.size(); ++i ) {
                LOG.debug("input split "+i+": "+splits.get(i));
            }
        }

        MapOutputInputStream input = new MapOutputInputStream(conf, taskid, inputByteCounter, secretKey, splits);
        reader = new MapOutputInputStreamReader<K,V>(input);
        
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
            useRawComp = ! ( myComp instanceof GroupComparator);

//            String compClsStr = conf.get(GROUP_COMPARATOR_CLASS, conf.get(KEY_COMPARATOR));
//            if ( compClsStr == null ) {
//                // something is wrong
//                throw new IllegalArgumentException("Can't find a comparator class!");
//            }
//            Class<? extends RawComparator<?>> compCls;
//            try {
//                compCls = (Class<? extends RawComparator<?>>) conf.getClassByName(compClsStr);
//            } catch (ClassNotFoundException e) {
//                throw new IllegalArgumentException("Can't load a comparator class: "+compClsStr);
//            }
//            comp = ReflectionUtils.newInstance(compCls, conf);
            this.comp = myComp;
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
//        boolean hasNextKey;
//        while ( (hasNextKey = reader.nextRawKey(buffer)) ) {
//            if ( comp == null
//                    || comp.compare(minKey, 0, minKey.length,
//                            buffer.getData(), 0, buffer.getLength()) < 0 ) {
//                break;
//            } else {
//                // otherwise, skip this key
//                reader.nextRawValue(buffer);
//            }
//        }
//        
//        if ( ! hasNextKey ) {
//            return false;
//        }
////        if ( !reader.nextRawKey(buffer) ) {
////            return false;
////        }
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
		return reader.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.reader.close();
        reader = null;
    }

    @Override
    public boolean next(K key, V value) throws IOException {
        boolean hasNextKey;
        /*
        while ( (hasNextKey = reader.nextRawKey(buffer)) ) {
            if ( comp == null
                    || comp.compare(minKey, 0, minKey.length,
                            buffer.getData(), 0, buffer.getLength()) < 0 ) {
                break;
            } else {
                // otherwise, skip this key
                reader.nextRawValue(buffer);
            }
        }
        */
        while ( (hasNextKey = reader.nextRawKey(buffer)) ) {
            if ( comp == null ) {
                this.key = keyDeserializer.deserialize(key);
                break;
            }
            
            if ( useRawComp ) {
                if ( comp.compare(minKey, 0, minKey.length, buffer.getData(), 0, buffer.getLength()) < 0 ) {
                    this.key = keyDeserializer.deserialize(key);
                    break;
                } 
            } else {
                this.key = keyDeserializer.deserialize(key);
                if ( comp.compare(minKeyObj, key) < 0 ) {
                    break;
                }
            }
            // otherwise, skip this key
            reader.nextRawValue(buffer);
            skipCounter.increment(1);
        }
        
        
        if ( ! hasNextKey ) {
            return false;
        }
//        if ( !reader.nextRawKey(buffer) ) {
//            return false;
//        }
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
        return reader.bytesRead;
    }
}
