package skewtune.mapreduce;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobPriority;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.security.TokenStorage;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;

import skewtune.mapreduce.JobInProgress.ReactionContext;
import skewtune.mapreduce.PartitionPlanner.Partition;
import skewtune.mapreduce.SimpleCostModel.Estimator;
import skewtune.mapreduce.lib.input.DelegateInputFormat;
import skewtune.mapreduce.lib.input.InputSplitCache;
import skewtune.mapreduce.lib.input.MapOutputKeyInputFormat;
import skewtune.mapreduce.lib.input.MapOutputSplit;
import skewtune.mapreduce.lib.input.RecordLength;
import skewtune.mapreduce.lib.input.SequenceFileProbeInputFormat;
import skewtune.utils.Base64;
import skewtune.utils.Utils;

/**
 * collect information related to map output for given reduce
 * 
 * TODO
 * 
 * implement a new serialization engine to avoid double encoding length information of key.
 * 
 * @author yongchul
 *
 */
public class PartitionMapInput implements SkewTuneJobConfig {
    public static final Log LOG = LogFactory.getLog(PartitionMapInput.class);
    public static final String MAX_BYTES_PER_MAP = "skewtune.partition.map.maxbytes";
    
    public static class MapInputPartition implements Partition {
        long offset;
        int numRecs;
        long lastOffset;
        
        private transient int index;
        private transient double cost;
        
        
        public MapInputPartition() {
            offset = -1;
        }
        
        public MapInputPartition(long off,long bytes) {
            offset = off;
            numRecs = 1;
            lastOffset = off + bytes;
        }
        
        public void reset() {
            offset = -1;
            numRecs = 0;
            lastOffset = -1;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeVLong(out, offset);
            WritableUtils.writeVInt(out,numRecs);
            WritableUtils.writeVLong(out, lastOffset - offset);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            offset = WritableUtils.readVLong(in);
            numRecs = WritableUtils.readVInt(in);
            lastOffset = offset + WritableUtils.readVLong(in);
        }

        @Override
        public long getLength() {
            return lastOffset - offset;
        }
        
        public long getOffset() {
            return offset;
        }
        
        public long getLastOffset() {
            return lastOffset;
        }
        
        public int getNumRecords() {
            return numRecs;
        }
        
        public void add(long off,RecordLength recLen) {
            if ( offset < 0 ) {
                offset = off;
            }
            ++numRecs;
            lastOffset = off + recLen.getRecordLength();
        }
        
        @Override
        public String toString() {
            return String.format("[%d,%d) recs=%d length=%d",offset,lastOffset,numRecs,lastOffset-offset);
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public void setIndex(int i) {
            this.index = i;
        }

        @Override
        public double getCost() {
            return cost;
        }

        @Override
        public void setCost(double c) {
            cost = c;
        }

        public void merge(MapInputPartition other) {
            if ( offset < 0 ) {
                offset = other.offset;
            }
            numRecs += other.numRecs;
            lastOffset = Math.max(lastOffset, other.lastOffset);
        }
    }

    /**
     * map input key: offset
     * map input value: record length
     * map output key: map output partition
     * @author yongchul
     */
    public static class Map extends Mapper<LongWritable,RecordLength,NullWritable,NullWritable> {
        public static final long DEFAULT_BYTES_PER_MAP = 1;
        private long maxBytesPerMap;
        private DataOutputStream pfOut;
        
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            Configuration conf = context.getConfiguration();
            maxBytesPerMap = conf.getLong(MAX_BYTES_PER_MAP, DEFAULT_BYTES_PER_MAP);
            
            String taskIdStr = conf.get(ORIGINAL_TASK_ID_ATTR);
            TaskID taskid = TaskID.forName(taskIdStr);
            String partitionFile = getPartitionFileName(taskid);
            Path dir = FileOutputFormat.getWorkOutputPath(context);
            Path fn = new Path(dir,partitionFile);
            FileSystem fs = fn.getFileSystem(conf);
            
            Class<?> codecClass = conf.getClass(JobContext.MAP_OUTPUT_COMPRESS_CODEC,DefaultCodec.class);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
//            DataOutputStream output = new DataOutputStream(codec.createOutputStream(buffer));
//            reducePlan.write(output);
//            output.close();

            pfOut = new DataOutputStream(codec.createOutputStream(fs.create(fn, true, 4*1024*1024)));
            
            LOG.info("input split = "+context.getInputSplit());
        }

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            
            MapInputPartition current = new MapInputPartition();
            while ( context.nextKeyValue() ) {
                LongWritable offset = context.getCurrentKey();
                RecordLength recLen = context.getCurrentValue();
                
                current.add(offset.get(), recLen);
                
                if ( recLen.isSplitable() ) {
                    if ( current.getLength() >= maxBytesPerMap ) {
                        current.write(pfOut);
                        current.reset();
                    }
                }
            }
            
            if ( current.getLength() > 0 ) {
                current.write(pfOut);
            }
            
            cleanup(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if ( pfOut != null ) pfOut.close();
            super.cleanup(context);
        }
    }
    
    public static String getPartitionFileName(TaskID taskid) {
        return String.format("partition-m-%05d", taskid.getId());
    }
    
    public static Path getOutputPath(JobInProgress jip,TaskID taskid) {
        Path orgOutDir = jip.getOriginalJob().getOutputPath();
        Path outDir = new Path(orgOutDir, String.format("scan-%s-%05d",taskid.getTaskType() == TaskType.MAP ? "m" : "r", taskid.getId()));
        return outDir;
    }
    
    public static Path getPartitionFile(JobInProgress jip,TaskID taskid) {
        Path outPath = getOutputPath(jip,taskid);
        return new Path(outPath,getPartitionFileName(taskid));
    }
    
    private void copyConfiguration(Configuration from,Configuration to,String name) {
        String v = from.get(name);
        if ( v != null && v.length() > 0 )
            to.set(name, v);
    }
    
    public org.apache.hadoop.mapreduce.Job prepareJob(JobInProgress jip,org.apache.hadoop.mapred.TaskID taskid,ReactionContext action, int maxSlots) throws IOException, InterruptedException {
        // prepare a job from given configuration
        org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(jip.getCluster());
        Configuration oldConf = jip.getConfiguration();
        Configuration newConf = job.getConfiguration();

        newConf.set(ORIGINAL_JOB_ID_ATTR, taskid.getJobID().toString());
        newConf.set(ORIGINAL_TASK_ID_ATTR, taskid.toString());
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
//        newConf.set(JobContext.COMBINE_CLASS_ATTR, null); // we don't need a combiner
        
        job.setJobName("Scan-"+taskid.toString());
     
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);
        
        job.setPriority(JobPriority.VERY_HIGH);
        
        job.setInputFormatClass(DelegateInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, getOutputPath(jip,taskid));
        
        newConf.set("mapreduce.job.parent",taskid.getJobID().toString());
        newConf.setBoolean("mapreduce.job.parent.sticky", true);
        newConf.setClass(ORIGINAL_INPUT_FORMAT_CLASS_ATTR,SequenceFileProbeInputFormat.class, InputFormat.class);
        newConf.setBoolean("mapred.mapper.new-api", true);
        
        // determine sample interval.
        // max( total bytes / (# of nodes * 10), 1)
        long sampleInterval = Math.max( action.getRemainBytes() / ( maxSlots*10), 1);
        if ( LOG.isInfoEnabled() ) {
            LOG.info(taskid+" sampling interval = "+sampleInterval);
        }
        newConf.setLong(SKEWTUNE_REPARTITION_SAMPLE_INTERVAL, sampleInterval);

        copyConfiguration(oldConf,newConf,MRJobConfig.CACHE_ARCHIVES);
        copyConfiguration(oldConf,newConf,MRJobConfig.CACHE_FILES);
        copyConfiguration(oldConf,newConf,MRJobConfig.CLASSPATH_ARCHIVES);
        copyConfiguration(oldConf,newConf,MRJobConfig.CLASSPATH_FILES);
        copyConfiguration(oldConf,newConf,MRJobConfig.CACHE_SYMLINK);
        copyConfiguration(oldConf,newConf,MRJobConfig.CACHE_FILE_TIMESTAMPS);
        copyConfiguration(oldConf,newConf,MRJobConfig.CACHE_ARCHIVES_TIMESTAMPS);
        copyConfiguration(oldConf,newConf,MRJobConfig.END_NOTIFICATION_URL);
        
        action.initScanTask(job);
        
        return job;
    }
    
    public static List<Partition> loadPartitionFile(FileSystem fs,Path file,Configuration conf) throws IOException {
        List<Partition> result = new ArrayList<Partition>();
        DataInputStream input = null;
        Class<?> codecClass = conf.getClass(JobContext.MAP_OUTPUT_COMPRESS_CODEC,DefaultCodec.class);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

        try {
            input = new DataInputStream(codec.createInputStream(fs.open(file, 4*1024*1024)));
            int i = 0;
            while ( true ) {
                MapInputPartition p = new MapInputPartition();
                p.readFields(input);
                result.add(p);
                p.setIndex(i++);
            }
        } catch ( EOFException ignore ) {
        } finally {
            if ( input != null ) {
                input.close();
            }
        }
        return result;
    }
    
    public static void main(String[] args) throws Exception {
        boolean isLocal = "-l".equals(args[0]) || "-local".equals(args[0]);
        Path file = new Path(isLocal ? args[1] : args[0]);
        
        Configuration conf = new Configuration();
        FileSystem fs = isLocal ? FileSystem.getLocal(conf) : file.getFileSystem(conf);
        
        List<Partition> parts = loadPartitionFile(fs,file,conf);
        System.out.println(parts.size() + " partitions loaded");
        for ( Partition p : parts ) {
            System.out.println(p);
        }
    }
}
