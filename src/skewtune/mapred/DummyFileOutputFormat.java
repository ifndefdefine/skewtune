package skewtune.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class DummyFileOutputFormat<K,V> extends FileOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
            String name, Progressable progress) throws IOException {
        return new RecordWriter<K,V>() {

            @Override
            public void write(K key, V value) throws IOException {
            }

            @Override
            public void close(Reporter reporter) throws IOException {
            }};
    }

}
