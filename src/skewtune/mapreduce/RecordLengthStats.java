package skewtune.mapreduce;

import hep.aida.bin.StaticBin1D;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import skewtune.mapreduce.lib.input.InputInfo;
import skewtune.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import skewtune.utils.BoundedPriorityQueue;

public class RecordLengthStats extends Configured implements Tool {
    private static final class RecordStats implements Writable {
        long n; // total number of records
        double keyAvg;
        double keyStdev;
        double valAvg;
        double valStdev;
        double recAvg;
        double recStdev;
        
        Record[] records;

        @Override
        public void readFields(DataInput in) throws IOException {
            n = in.readLong();
            recAvg = in.readDouble();
            recStdev = in.readDouble();
            keyAvg = in.readDouble();
            keyStdev = in.readDouble();
            valAvg = in.readDouble();
            valStdev = in.readDouble();
            
            records = new Record[ in.readInt() ];
            for ( int i = 0; i < records.length; ++i ) {
                records[i] = new Record();
                records[i].readFields(in);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(n);
            out.writeDouble(recAvg);
            out.writeDouble(recStdev);
            out.writeDouble(keyAvg);
            out.writeDouble(keyStdev);
            out.writeDouble(valAvg);
            out.writeDouble(valStdev);
            
            out.writeInt(records.length);
            for ( Record r : records )
                r.write(out);
        }
        
        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder(1024);
            buf.append(String.format("%d,%g,%g,%g,%g,%g,%g;",n,recAvg,recStdev,keyAvg,keyStdev,valAvg,valStdev));
            for ( Record r : records ) {
                buf.append(r);
                buf.append(';');
            }
            return buf.toString();
        }
    }
    
    public static final class Record implements Writable, Comparable<Record> {
        Path file;
        long pos;
        int keyLen;
        int valLen;
        
        public Record() {}
        
        public Record(Path f,long p,int k,int v) {
            this.file = f;
            this.pos = p;
            this.keyLen = k;
            this.valLen = v;
        }
        
        public Path getFile() { return file; }
        public long getPosition() { return pos; }
        public int getKeyLength() { return keyLen; }
        public int getValueLength() { return valLen; }
        public int getRecordLength() { return keyLen+valLen; }
        
        @Override
        public void readFields(DataInput in) throws IOException {
            file = new Path(Text.readString(in));
            pos = in.readLong();
            keyLen = in.readInt();
            valLen = in.readInt();
        }
        
        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, file.toString());
            out.writeLong(pos);
            out.writeInt(keyLen);
            out.writeInt(valLen);
        }
        
        @Override
        public String toString() {
            return String.format("%s,%d,%d,%d", file.toString(),pos,keyLen,valLen);
        }

        /**
         * order by record length. if record length is the same, in the order of file name, value length, then key length
         */
        @Override
        public int compareTo(Record r) {
            int rc = getRecordLength() - r.getRecordLength();
            if ( rc == 0 ) {
                rc = file.compareTo(r.getFile());
                if ( rc == 0 ) {
                    rc = valLen - r.getValueLength();
                    if ( rc == 0 )
                        rc = keyLen - r.getKeyLength();
                }
            }
            return rc;
        }
    }
    
    static final int NUMSAMPLE_PER_PARTITION = 10;

	private static class MapClass extends Mapper<BytesWritable, BytesWritable, IntWritable, RecordStats> {
        StaticBin1D keyStat = new StaticBin1D();
        StaticBin1D valStat = new StaticBin1D();
        StaticBin1D recStat = new StaticBin1D();
        
        InputInfo info;
        
        BoundedPriorityQueue<Record> q;
        Record minRecord;
        int partition;
        
        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            info = InputInfo.getInstance();
            int k = context.getConfiguration().getInt("skewtune.recordstat.samplePerPartition", NUMSAMPLE_PER_PARTITION);
            q = new BoundedPriorityQueue<Record>(k);
            partition = context.getConfiguration().getInt("mapreduce.task.partition", -1);
            if ( partition < 0 )
                partition = context.getConfiguration().getInt("mapred.task.partition", -1);
            if ( partition < 0 )
                System.err.println("Unabled to locate partition number");
        }

		@Override
        protected void map(BytesWritable key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
		    int keyLen = key.getLength();
		    int valLen = value.getLength();
		    int recLen = keyLen + valLen; 

		    keyStat.add(keyLen);
		    valStat.add(valLen);
		    recStat.add(recLen);
		    
		    minRecord = q.peek();
		    if ( ! q.isFull() || minRecord == null || minRecord.getRecordLength() < recLen ) {
		        q.add(new Record(info.getSplit().getPath(),info.getPosition(),keyLen,valLen));
		    }
        }
		
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            RecordStats stats = new RecordStats();
            stats.n = keyStat.size();
            stats.keyAvg = keyStat.mean();
            stats.keyStdev = keyStat.standardDeviation();
            stats.valAvg = valStat.mean();
            stats.valStdev = valStat.standardDeviation();
            stats.recAvg = recStat.mean();
            stats.recStdev = recStat.standardDeviation();
            
            stats.records = q.toArray(new Record[q.size()]);
            
            context.write(new IntWritable(partition), stats);
        }
	}
	
	private static class ReduceClass extends Reducer<IntWritable, RecordStats, IntWritable, IntWritable> {
	    int numPartitions;
	    BoundedPriorityQueue<Record> q;
	    
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            numPartitions = context.getConfiguration().getInt("mapreduce.job.maps", -1);
            if ( numPartitions < 0 )
                System.err.println("Can't determine number of maps");
            
            // if q should have return something?
        }

        @Override
        protected void reduce(IntWritable numRecs, Iterable<RecordStats> values,Context context)
                throws IOException, InterruptedException {
            // 
		}
        
        /*
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
        }
        */
	}	
	
	
	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RecordLengthStats(), args);
		System.exit(res);
	}

	public RecordLengthStats() {
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
        boolean bad = false;
        int i = 0;
        String output = null;
        
        for ( ; i < args.length; ++i ) {
            if ( args[i].charAt(0) != '-' )
                break;
            
            if ( "-output".equals(args[i]) ) {
                output = args[++i];
            } else {
                System.err.println("Unknown option: "+args[i]);
                bad = true;
            }
        }
        
        if ( bad ) {
            System.exit(1);
        }
        
        if ( output == null ) {
            output = "temp-partition";
            System.err.println("WARNING: converted data will be written under "+output);
        }
        
        Path outPath = new Path(output);
        FileSystem fs = FileSystem.get(getConf());
        if ( fs.exists(outPath) ) {
            fs.delete(outPath, true);
        }

        Job job2 = new Job(getConf(),"CollectRecordStats");

        for ( ; i < args.length; ++i ) {
            FileInputFormat.addInputPaths(job2,args[i]);
        }
        FileOutputFormat.setOutputPath(job2, outPath);
        
        job2.setJarByClass(RecordLengthStats.class);

        job2.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(RecordStats.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(RecordStats.class);

        job2.setMapperClass(MapClass.class);
        job2.setReducerClass(Reducer.class);
        
        job2.setNumReduceTasks(1);
        
        job2.submit();

        return job2.waitForCompletion(true) ? 0 : -1;
	}
}
