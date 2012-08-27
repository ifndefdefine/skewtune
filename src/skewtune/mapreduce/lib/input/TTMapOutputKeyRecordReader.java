package skewtune.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
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
import skewtune.utils.Base64;

public class TTMapOutputKeyRecordReader
    extends RecordReader<BytesWritable,IntWritable>
    implements MRJobConfig, SkewTuneJobConfig, 
            org.apache.hadoop.mapred.RecordReader<BytesWritable,IntWritable> {
    private static final Log LOG = LogFactory.getLog(TTMapOutputRecordReader.class);

    protected Configuration conf;
    private Counter inputByteCounter;

    private BytesWritable key = new BytesWritable();
    private IntWritable value = new IntWritable();
    
    private MapOutputInputStreamReader<BytesWritable,IntWritable> reader;
    private DataInputBuffer buffer = new DataInputBuffer();

    
    @SuppressWarnings("unchecked")
    @Override
    public void initialize(InputSplit split,TaskAttemptContext context) throws IOException, InterruptedException {
        MapContext<BytesWritable, IntWritable, ?, ?> mapContext = (MapContext<BytesWritable, IntWritable, ?, ?>)context;
        Configuration conf = mapContext.getConfiguration();
        
        initialize(mapContext.getConfiguration(),
                mapContext.getCounter(FileInputFormat.COUNTER_GROUP,FileInputFormat.BYTES_READ),
                split
                );
    }
    
    @SuppressWarnings("unchecked")
    public void initialize(org.apache.hadoop.mapred.InputSplit split, JobConf job,Reporter reporter) throws IOException, InterruptedException {
        initialize(job,
                reporter.getCounter(FileInputFormat.COUNTER_GROUP, FileInputFormat.BYTES_READ),
                split
                );
    }
    
    
    private void initialize(Configuration conf,Counter counter,Object split)
    throws IOException, InterruptedException {
        this.conf = conf;
        this.inputByteCounter = counter;  
        
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
        reader = new MapOutputInputStreamReader<BytesWritable,IntWritable>(input);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return next(key,value);
    }

    @Override
    public BytesWritable getCurrentKey() {
        return key;
    }

    @Override
    public IntWritable getCurrentValue() {
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
    public boolean next(BytesWritable key, IntWritable value) throws IOException {
        long start = getPos();
        boolean hasNextKey = reader.nextRawKey(buffer);
        if ( ! hasNextKey ) {
            return false;
        }
        key.set(buffer.getData(), 0, buffer.getLength());
        reader.nextRawValue(buffer);
        int length = (int)(getPos() - start);
        value.set(length);
        return true;
    }

    @Override
    public BytesWritable createKey() {
        return key;
    }

    @Override
    public IntWritable createValue() {
        return value;
    }

    @Override
    public long getPos() throws IOException {
        return reader == null ? 0 : reader.getPosition();
    }
}
