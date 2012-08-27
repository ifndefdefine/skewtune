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

package skewtune.mapreduce.lib.input;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.IFile.Reader;

/**
 * From org.apache.hadoop.mapreduce.task.reduce
 * 
 * <code>IFile.InMemoryReader</code> to read map-outputs present in-memory.
 */
class MapOutputInputStreamReader<K, V> extends Reader<K, V> {
    public static final Log LOG = LogFactory.getLog(MapOutputInputStreamReader.class);

    private final MapOutputInputStream inputStream;
    private final DataInputStream dataInput;

    private byte[] keyBuf = new byte[8192];
    private byte[] valBuf = new byte[8192];

    public MapOutputInputStreamReader(MapOutputInputStream input)
            throws IOException {
        super(null, null, 0, null, null); // FIXME fix it later?
        this.inputStream = input;
        this.dataInput = new DataInputStream(inputStream);
    }

    @Override
    public void reset(int offset) {
        throw new UnsupportedOperationException(); // suicide
    }

    @Override
    public long getPosition() throws IOException {
        // InMemoryReader does not initialize streams like Reader, so
        // in.getPos()
        // would not work. Instead, return the number of uncompressed bytes
        // read,
        // which will be correct since in-memory data is not compressed.
        return bytesRead;
    }

    @Override
    public long getLength() {
        return 0; // FIXME
    }
    
    public float getProgress() {
        return inputStream.getProgress();
    }

    public boolean nextRawKey(DataInputBuffer key) throws IOException {
        try {
            while ( !positionToNextRecord(dataInput) ) {
                // Okay, we hit the one end of map-output. check whether
                // this is really true
                
                eof = ! this.inputStream.hasMore(); // if zero, that is we have completed
                
                if ( eof ) {
                    return false;
                }
                
                // if we have more, then the following call will place the pointer to next map output
                if ( LOG.isDebugEnabled() ) {
                    LOG.debug("reading output of "+inputStream.getCurrentTask()+" from "+inputStream.getCurrentHost());
                }
            }
            
//            if ( LOG.isDebugEnabled() ) {
//                LOG.debug(String.format("current key length = %d; current value length = %d", currentKeyLength, currentValueLength) );
//            }

            // Setup the key
            keyBuf = keyBuf.length < currentKeyLength ? new byte[currentKeyLength << 1] : keyBuf;
            dataInput.readFully(keyBuf, 0, currentKeyLength);
            key.reset(keyBuf, 0, currentKeyLength);
            
//            if ( LOG.isDebugEnabled() ) {
//                LOG.debug("key = "+Utils.toHex(keyBuf,0,currentKeyLength));
//            }

            bytesRead += currentKeyLength;
            return true;
        } catch ( EOFException eofex ) {
            if ( this.inputStream.hasMore() ) { // double check
                throw eofex; // SOMETHING IS WRONG?
            } else {
                return false;
            }
        } catch (IOException ioe) {
            throw ioe;
        }
    }

    public void nextRawValue(DataInputBuffer value) throws IOException {
        try {
            valBuf = valBuf.length < currentValueLength ? new byte[currentValueLength << 1] : valBuf;
            dataInput.readFully(valBuf, 0, currentValueLength);
            value.reset(valBuf, 0, currentValueLength);
//            if ( LOG.isDebugEnabled() ) {
//                LOG.debug("value = "+Utils.toHex(valBuf,0,currentValueLength));
//            }
            bytesRead += currentValueLength;
            ++recNo;
        } catch (IOException ioe) {
            throw ioe;
        }
    }

    public void close() {
        try {
            dataInput.close();
        } catch (IOException ex) {
            // FIXME dump
        }
    }
}
