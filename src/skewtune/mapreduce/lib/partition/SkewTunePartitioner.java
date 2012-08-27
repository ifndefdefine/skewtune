package skewtune.mapreduce.lib.partition;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.SkewTuneJobConfig;
import skewtune.utils.Base64;

public abstract class SkewTunePartitioner<K,V>
extends Partitioner<K, V>
implements Configurable, MRJobConfig, SkewTuneJobConfig, org.apache.hadoop.mapred.Partitioner<K,V> {
    
    @Override
    public void configure(JobConf conf) {
        // DO NOTHING. we will initialize in setConf()
//        setConf(conf);
    }

    @Override
    public Configuration getConf() {
        throw new UnsupportedOperationException();
    }

    protected DataInputStream getBucketInfo(Configuration conf) throws IOException {
        String enc = conf.get(SKEWTUNE_REPARTITION_BUCKETS);
        byte[] data = Base64.decode(enc);
        DataInputBuffer bucketData = new DataInputBuffer();
        bucketData.reset(data,data.length);

        Class<?> codecClass = conf.getClass(JobContext.MAP_OUTPUT_COMPRESS_CODEC,DefaultCodec.class);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        return new DataInputStream(codec.createInputStream(bucketData));
    }

    /**
     * return the scheduling order. the returned array is indexed by the key range partition
     * and the value is the actual task id (schedule)
     * @param conf
     * @return
     * @throws IOException
     */
    protected int[] getScheduleOrder(Configuration conf) throws IOException {
        String enc = conf.get(SKEWTUNE_REPARTITION_SCHEDULES);
        int numReduces = conf.getInt(JobContext.NUM_REDUCES, 1);
        int[] order = new int[numReduces];

        if ( enc == null || enc.length() == 0 ) {
        	for ( int i = 0; i < numReduces; ++i ) {
        		order[i] = i;
        	}
        } else {
	        byte[] data = Base64.decode(enc);
	        DataInputBuffer bucketData = new DataInputBuffer();
	        bucketData.reset(data,data.length);
	
	        Class<?> codecClass = conf.getClass(JobContext.MAP_OUTPUT_COMPRESS_CODEC,DefaultCodec.class);
	        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
	        DataInputStream input = new DataInputStream(codec.createInputStream(bucketData));
	        
	        for ( int i = 0; i < numReduces; ++i ) {
	        	order[WritableUtils.readVInt(input)] = i;
	        }
	    	input.close();
        }
    	
    	return order;
    }
}
