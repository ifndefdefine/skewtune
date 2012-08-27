package org.apache.hadoop.mapred;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;

import skewtune.mapreduce.lib.input.RecordLength;

public interface SplittableRecordReader<K,V> extends RecordReader<K, V> {
    /**
     * tell current position and stop
     * 
     * @param out information about current position of input split
     * @throws IOException
     */
    public StopStatus tellAndStop(DataOutput out) throws IOException;
}