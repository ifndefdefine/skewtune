package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import skewtune.mapreduce.lib.input.RecordLength;

public interface ScannableReader<K,V> {
    public RelativeOffset getRelativeOffset();
    public void initScan() throws IOException;
    public boolean scanNextKeyValue(LongWritable offset,RecordLength recLen) throws IOException;
    public void closeScan() throws IOException;
}
