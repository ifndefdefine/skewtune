/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package skewtune.mapreduce.lib.output;

import java.io.IOException;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** An {@link OutputFormat} that writes {@link SequenceFile}s. */

public final class DummyFileOutputFormat<K, V> extends FileOutputFormat<K, V> {
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new RecordWriter<K, V>() {

            @Override
            public void write(K key, V value) throws IOException {
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException {
            }
        };
    }

}
