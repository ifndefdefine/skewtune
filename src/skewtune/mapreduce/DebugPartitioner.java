package skewtune.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import skewtune.mapreduce.lib.partition.GroupComparator;
import skewtune.mapreduce.lib.partition.RangePartitioner;
import skewtune.utils.Utils;

public class DebugPartitioner extends Configured implements Tool {
    
    public static class CloudBurstGroupComp extends WritableComparator {
        final BytesWritable.Comparator comp = new BytesWritable.Comparator();
        
        public CloudBurstGroupComp() {
            super(BytesWritable.class);
        }
        
        @Override
        public int compare(WritableComparable o1,WritableComparable o2) {
            BytesWritable bw1 = (BytesWritable)o1;
            BytesWritable bw2 = (BytesWritable)o2;
            
            byte[] b1 = bw1.getBytes();
            byte[] b2 = bw2.getBytes();
            
            int l1 = bw1.getLength() - 1;
            for ( int i = 0; i < l1; ++i ) {
                int diff = b1[i] - b2[i];
                if ( diff != 0 ) return diff;
            }
            return 0;
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return comp.compare(b1,s1,l1-1,b2,s2,l2-1);
        }
    }
    
    private int dumpFile(boolean local,Path path) throws IOException {
        FileSystem fs = local ? FileSystem.getLocal(getConf()) : path.getFileSystem(getConf());
        
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, getConf());
        BytesWritable key = new BytesWritable();
        IntWritable val = new IntWritable();
        
        while ( reader.next(key, val) ) {
            System.out.println(Utils.toHex(key.getBytes(),0,key.getLength())+","+val.get());
        }
        
        reader.close();
        
        return 0;
    }
    
    private int concat(boolean local,Path in,Path out) throws IOException {
        FileSystem fs = local ? FileSystem.getLocal(getConf()) : in.getFileSystem(getConf());

        BytesWritable key = new BytesWritable();
        IntWritable val = new IntWritable();
        
        if ( fs.exists(out) ) {
            fs.delete(out, false);
        }

        SequenceFile.Writer writer = new SequenceFile.Writer(fs, getConf(), out, BytesWritable.class, IntWritable.class);
        for ( FileStatus status : fs.globStatus(in) ) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, status.getPath(), getConf());
            while ( reader.next(key, val) ) {
                writer.append(key, val);
            }
            reader.close();
        }
        
        writer.close();
        
        return 0;
    }
    
    private int test(boolean local, Path path, String buckets) throws IOException {
        FileSystem fs = local ? FileSystem.getLocal(getConf()) : path.getFileSystem(getConf());

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, getConf());
        BytesWritable key = new BytesWritable();
        IntWritable val = new IntWritable();
        
        ArrayList<BytesWritable> b = new ArrayList<BytesWritable>();
        for ( String s : "466c000000,822cc00000,b9ab800000,fff5800001,ffffc02201".split(",") ) {
            b.add(new BytesWritable(Utils.toBinary(s)));
        }
        
        BitSet exclude = new BitSet(b.size());
        exclude.set(1);
        exclude.set(3);
        exclude.set(0);
        
        int[] mappings = new int[] { 4, 0, 1, 3, 2, 5 };
        GroupComparator<BytesWritable> grpcomp = new GroupComparator<BytesWritable>(new BytesWritable.Comparator(),new CloudBurstGroupComp());
        RangePartitioner<BytesWritable,IntWritable> part = new RangePartitioner<BytesWritable,IntWritable>(b,grpcomp,mappings,exclude);
        BytesWritable meh = new BytesWritable();

        int prev = -1;
        int count = 0;
        while ( reader.next(key, val) ) {
            if ( ++count < 4000 ) {
                meh.set(key.getBytes(),4,key.getLength()-4);
                int p = part.getPartition(meh, val, mappings.length);
//                if ( p != prev ) {
                    System.err.println(Utils.toHex(meh.getBytes(),0,meh.getLength())+";prev="+prev+";now="+p);
                    prev = p;
//                }
            }
        }
        
        reader.close();
        
        return 0;
    }
    
    enum Action {
        DUMP,
        CONCAT,
        TEST,
        PREPARE
    }


    @Override
    public int run(String[] args) throws Exception {
        int numReduces = 1;
        String buckets = null;
        boolean local = false;
        Action action = Action.PREPARE;
        int i = 0;
        for ( ; i < args.length; ++i ) {
            String arg = args[i];
            if ( arg.charAt(0) != '-' ) break;
            if ( arg.equals("-numReduce") ) {
                numReduces = Integer.parseInt(args[++i]);
            } else if ( arg.equals("-bucket") ) {
                buckets = args[++i];
            } else if ( arg.equals("-local") ) {
                local = true;
            } else {
                try {
                    action = Enum.valueOf(Action.class, arg.substring(1).toUpperCase() );
                } catch ( IllegalArgumentException x ) {}
                System.err.println("Unknown option : "+arg);
            }
        }
        
        Path inPath = new Path(args[i++]);
        
        if ( action == Action.DUMP ) {
            return dumpFile(local,inPath);
        } else if ( action == Action.TEST ) {
            return test(local,inPath,buckets);
        } else if ( action == Action.CONCAT ) {
            Path outPath = new Path(args[i++]);
            return concat(local,inPath,outPath);
        } else {
            Path outPath = new Path(args[i++]);
            
            Cluster cluster = new Cluster(getConf());
            org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(cluster);
            
            job.getConfiguration().set("skewtune.partition.bucket", buckets);
            
            job.setMapperClass(Mapper.class);
            job.setReducerClass(Reducer.class);
            
            if ( numReduces == 1 ) {
                job.setPartitionerClass(HashPartitioner.class);
            } else {
                job.setPartitionerClass(RangePartitioner.class);
            }
            
            job.setGroupingComparatorClass(CloudBurstGroupComp.class);
            job.setOutputKeyClass(BytesWritable.class);
            job.setOutputValueClass(IntWritable.class);
            
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            
            job.setNumReduceTasks(numReduces);
            
            FileSystem fs = outPath.getFileSystem(getConf());
            if ( fs.exists(outPath) ) {
                fs.delete(outPath, true);
            }
            
            FileInputFormat.addInputPath(job, inPath);
            FileOutputFormat.setOutputPath(job, outPath);
            
            job.submit();
            
            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new DebugPartitioner(),args);
        System.exit(rc);
    }
}
