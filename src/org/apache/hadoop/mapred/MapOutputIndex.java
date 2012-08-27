package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.IndexRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.PureJavaCrc32;

/**
 * A data structure to represent offset-length information in the map output.
 * Each entry corresponds to output destined to a reducer of which id is the same as entry offset in the data structure.
 * 
 * @author yongchul
 *
 */
public class MapOutputIndex implements Writable {
    /** Backing store */
    private ByteBuffer buf;
    /** View of backing storage as longs */
    private LongBuffer entries;

    public static class Record implements Writable {
        long startOffset;
        long rawLength;
        long partLength;

        public Record() { }

        public Record(long startOffset, long rawLength, long partLength) {
          this.startOffset = startOffset;
          this.rawLength = rawLength;
          this.partLength = partLength;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(startOffset);
            out.writeLong(rawLength);
            out.writeLong(partLength);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            startOffset = in.readLong();
            rawLength = in.readLong();
            partLength = in.readLong();
        }
        
        public long getOffset() { return startOffset; }
        public long getRawLength() { return rawLength; }
        public long getPartLength() { return partLength; }
        
        @Override
        public String toString() {
            return String.format("start=%d,length=%d,compressed=%d",startOffset,rawLength,partLength);
        }
    }
    
    public MapOutputIndex() {}
   
    public MapOutputIndex(Path indexFileName, JobConf job,boolean local) throws IOException {
        final FileSystem fs = local ? FileSystem.getLocal(job).getRaw() : FileSystem.get(job);
        final FSDataInputStream in = fs.open(indexFileName);
        try {
            Checksum crc = local ? new PureJavaCrc32() : null;
            final long length = local ? fs.getFileStatus(indexFileName).getLen() : in.readInt(); // will save an RPC
            final int partitions = (int) length / MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
            final int size = partitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
    
            buf = ByteBuffer.allocate(size);
            if (crc != null) {
                crc.reset();
                CheckedInputStream chk = new CheckedInputStream(in, crc);
                IOUtils.readFully(chk, buf.array(), 0, size);
                if (chk.getChecksum().getValue() != in.readLong()) {
                    throw new ChecksumException(
                            "Checksum error reading spill index: "
                                    + indexFileName, -1);
                }
            } else {
                IOUtils.readFully(in, buf.array(), 0, size);
            }
            entries = buf.asLongBuffer();
        } finally {
          in.close();
        }
    }
    
    public MapOutputIndex(int numPartitions) {
        buf = ByteBuffer.allocate(numPartitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
        entries = buf.asLongBuffer();
    }
    
    public MapOutputIndex(SpillRecord sr) {
        int numPartitions = sr.size();
        buf = ByteBuffer.allocate(numPartitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
        entries = buf.asLongBuffer();
        
        for ( int i = 0; i < numPartitions; ++i ) {
            putIndex(sr.getIndex(i),i);
        }
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        int bytes = in.readInt();
        byte[] tmp = new byte[bytes];
        in.readFully(tmp);
        buf = ByteBuffer.wrap(tmp);
        entries = buf.asLongBuffer();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int bytes = entries.capacity() * 8;
        out.writeInt(bytes);
        out.write(buf.array(),buf.arrayOffset(),bytes);
    }
    

    /**
     * Return number of IndexRecord entries in this spill.
     */
    public int size() {
      return entries.capacity() / (MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8);
    }

    /**
     * Get spill offsets for given partition.
     */
    public Record getIndex(int partition) {
      final int pos = partition * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
      return new Record(entries.get(pos), entries.get(pos + 1),
                             entries.get(pos + 2));
    }

    /**
     * Set spill offsets for given partition.
     */
    public void putIndex(IndexRecord rec, int partition) {
      final int pos = partition * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
      entries.put(pos, rec.startOffset);
      entries.put(pos + 1, rec.rawLength);
      entries.put(pos + 2, rec.partLength);
    }
    
    /**
     * copy 'from' index entry of src to 'to' entry of this index 
     * @param srcIdx
     * @param from
     * @param to
     */
    public void copyIndex(MapOutputIndex src,int from,int to) {
        final int srcPos = (from * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
        final int dstPos = (to * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
        System.arraycopy(src.buf.array(), srcPos, buf.array(), dstPos, MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
    }
    
    public long getTotalRawLength() {
        final int sz = size();
        final int perRec = MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
        long length = 0;
        for ( int i = 1; i < sz; i += perRec ) {
            length += entries.get(i);
        }
        return length;
    }
    
    public long getTotalCompressedLength() {
        final int sz = size();
        final int perRec = MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
        long length = 0;
        for ( int i = 2; i < sz; i += perRec ) {
            length += entries.get(i);
        }
        return length;
    }
    
    /**
     * copy entire content of src to this object starting pos. assuming all supplied src instances has the same length.
     * @param pos
     * @param src
     */
    public void put(int pos,MapOutputIndex src) {
        final byte[] srcBuf = src.buf.array();
        final int dstPos = srcBuf.length * pos;
        System.arraycopy(srcBuf, 0, buf.array(), dstPos, srcBuf.length);
    }
    
    public void writeToFile(Path loc, JobConf job, short rep)
    throws IOException {
      final FileSystem fs = FileSystem.get(job);
      final FSDataOutputStream out = fs.create(loc,rep);
      try {
          write(out);
      } finally {
          out.close();
      }
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        for ( int i = 0; i < size(); ++i ) {
            buf.append(getIndex(i));
            buf.append(';');
        }
        return buf.toString();
    }
}
