package skewtune.mapreduce.lib.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.MapOutputIndex;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;

/**
 * either holding a tasktracker information
 * or reactive output information in HDFS
 * 
 * @author yongchul
 */
public class MapOutputSplit extends InputSplit implements Writable, Iterable<TaskAttemptID>, org.apache.hadoop.mapred.InputSplit {
    // host is common for both type
    private String host;

    // task tracker information
    private String baseUrl;
    private JobID  jobid;
    private ArrayList<TaskAttemptID> attempts = new ArrayList<TaskAttemptID>();
    
    // reactive output information
    private ArrayList<ReactiveOutputSplit> files = new ArrayList<ReactiveOutputSplit>();
    
    private transient long lengthOfSplit; // length of compressed split
    private transient long rawLengthOfSplit;
    private transient long ioLengthOfSplit;
    
    public static class ReactiveOutputSplit implements Writable, Comparable<ReactiveOutputSplit> {
        private int mapId; // original map id
        private int numSplits;
        private int partid; // actual partition id
        private MapOutputIndex.Record index; // the location of map output
        private long offset; // offset in decompressed stream
        private long length; // length in decompressed stream
        
        public ReactiveOutputSplit() {
            index = new MapOutputIndex.Record();
        }
        
        public ReactiveOutputSplit(int m,int n,int p,MapOutputIndex.Record index) {
            this.mapId = m;
            this.numSplits = n;
            this.partid = p;
            this.index = index;
            this.offset = 0;
            this.length = this.index.getRawLength();
        }
        
        public ReactiveOutputSplit(int m,int n,int p,MapOutputIndex.Record index,long off,long len) {
            this.mapId = m;
            this.numSplits = n;
            this.partid = p;
            this.index = index;
            this.offset = off;
            this.length = len;
        }
        
        public int getMapID() { return mapId; }
        public int getNumSplits() { return numSplits; }
        public int getPartID() { return partid; }
        
        public MapOutputIndex.Record getOutputIndex() { return index; }
        
        public long getOffset() { return offset; }
        public long getLength() { return length; }
        
        public long getRawLength() { return index.getRawLength(); }
        public long getCompressedOffset() { return index.getOffset(); }
        public long getCompressedLength() { return index.getPartLength(); }
        
        // total number of bytes to process this split
        public long getIOLength() {
            return offset+length; // since we have to scan...
        }
        
        public Path getPath(Path dir) {
            return new Path(String.format("%s/m-%05d-%d/part-%05d",dir.toString(),mapId,numSplits,partid));
        }

        @Override
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeVInt(out, mapId);
            WritableUtils.writeVInt(out, numSplits);
            WritableUtils.writeVInt(out, partid);
            index.write(out);
            out.writeLong(offset);
            out.writeLong(length);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            mapId = WritableUtils.readVInt(in);
            numSplits = WritableUtils.readVInt(in);
            partid = WritableUtils.readVInt(in);
            index.readFields(in);
            offset = in.readLong();
            length = in.readLong();
        }
        
        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("map=").append(mapId).append(';');
            buf.append("nsplit=").append(numSplits).append(';');
            buf.append("partid=").append(partid).append(';');
            buf.append("index=").append(index).append(';');
            buf.append('[').append(offset).append(',').append(offset+length).append(')');
            return buf.toString();
        }

        /**
         * -1 if this chunk handles more data than the other
         * @param other
         * @return
         */
        @Override
        public int compareTo(ReactiveOutputSplit other) {
            return Long.signum(other.length - length);
        }
    }
    
    public MapOutputSplit() {}
    
    public MapOutputSplit(String host,String baseUrl) {
        this.host = host;
        this.baseUrl = baseUrl;
    }
    
    public MapOutputSplit(String host) {
        this.host = host;
    }

    public void add(TaskAttemptID id) {
        if ( attempts.isEmpty() ) {
            jobid = id.getJobID();
        } else if ( ! jobid.equals(id.getJobID()) ) {
            throw new IllegalArgumentException("jobid does not match. excepted: "+jobid);
        }
        attempts.add(id);
    }
    
    public void add(int mapid,int nsplit,int part,MapOutputIndex.Record index) {
        ReactiveOutputSplit split = new ReactiveOutputSplit(mapid,nsplit,part,index);
        files.add(split);
        lengthOfSplit += split.getCompressedLength();
        rawLengthOfSplit += split.getLength();
        ioLengthOfSplit += split.getIOLength();
    }
    
    public void add(int mapid,int nsplit,int part,MapOutputIndex.Record index,long off,long len) {
        ReactiveOutputSplit split = new ReactiveOutputSplit(mapid,nsplit,part,index,off,len);
        files.add(split);
        lengthOfSplit += split.getCompressedLength();
        rawLengthOfSplit += split.getLength();
        ioLengthOfSplit += split.getIOLength();
    }

    
    public boolean isReactiveOutputSplit() { return baseUrl == null && jobid == null && attempts.isEmpty(); }

    public String getHost() {
        return host;
    }
    
    public JobID getJobId() {
        return jobid;
    }
    
    public int size() {
        return attempts.size();
    }
        
    @Override
    public long getLength() {
        return Math.max(ioLengthOfSplit,attempts.size());
    }
    
    public long getRawLength() {
        return rawLengthOfSplit;
    }

    @Override
    public String[] getLocations() {
        return ( host == null || host.length() == 0 ) ? new String[0] : new String[] { host };
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        host = Text.readString(in);
        
        byte type = in.readByte();
        
        if ( type == 0 ) {
            int n = WritableUtils.readVInt(in);
            files.ensureCapacity(n);
            files.clear();
            lengthOfSplit = 0;
            for ( int i = 0; i < n; ++i ) {
                ReactiveOutputSplit split = new ReactiveOutputSplit();
                split.readFields(in);
                files.add(split);
                lengthOfSplit += split.getCompressedLength();
                rawLengthOfSplit += split.getLength();
                ioLengthOfSplit += split.getIOLength();
            }
        } else {
            baseUrl = Text.readString(in);
            jobid = JobID.forName(Text.readString(in));
            
            int n = WritableUtils.readVInt(in);
            attempts.ensureCapacity(n);
            attempts.clear();
            
            final String jtid = jobid.getJtIdentifier();
            final int jid = jobid.getId();
            for ( int i = 0; i < n; ++i ) {
                int mapid = WritableUtils.readVInt(in);
                int attempt = in.readUnsignedByte();
                attempts.add( new org.apache.hadoop.mapred.TaskAttemptID(jtid,jid,TaskType.MAP,mapid,attempt) );
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if ( host == null ) {
            Text.writeString(out, "");
        } else {
            Text.writeString(out, host);
        }
        
        if ( baseUrl == null ) {
            out.write(0);
            WritableUtils.writeVInt(out, files.size());
            for ( ReactiveOutputSplit split : files ) {
                split.write(out);
            }
        } else {
            out.write(1);
            Text.writeString(out, baseUrl);
            Text.writeString(out, attempts.get(0).getJobID().toString());
            WritableUtils.writeVInt(out, attempts.size());
            for ( TaskAttemptID tid : attempts ) {
                WritableUtils.writeVInt(out,tid.getTaskID().getId());
                out.writeByte(tid.getId());
            }
        }
    }
    
    @Override
    public Iterator<TaskAttemptID> iterator() {
        return attempts.iterator();
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public List<TaskAttemptID> getMaps() {
        return attempts;
    }
    
    public List<ReactiveOutputSplit> getReactiveOutputs() {
        return files;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
