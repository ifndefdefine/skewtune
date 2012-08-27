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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapOutputIndex;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * From Hadoop Shuffle code.
 * 
 * @author yongchul
 * 
 * @param <K>
 * @param <V>
 */

// FIXME the job secret key is not correctly set
// FIXME when there is error, it does not halt
class MapOutputInputStream extends java.io.InputStream {
    private static final Log LOG = LogFactory.getLog(MapOutputInputStream.class);

    private final SecretKey jobTokenSecret;
    private final Counter inputCounter;
    private final TaskID  reduceTaskId;
    private ByteBuffer buf;

    private boolean closed;

    volatile IOException ioException;

    private final ArrayBlockingQueue<ByteBuffer> q;

    private final int PACKET_SIZE;

    private final ByteBuffer[] buffers;

    private int nextBuf;

    private final List<MapOutputSplit> splits;

    private final Fetcher fetcher;

    // Decompression of map-outputs
    private final CompressionCodec codec;

    private final Decompressor decompressor;

    private Progress progress;
    
    private long bytesRead;

    // status
    AtomicLong totalBytes = new AtomicLong();
    AtomicBoolean more = new AtomicBoolean(true);
//    private volatile boolean more = true; // has more map or split?
    private volatile String currentHost; // currently retrieving host
    private volatile TaskAttemptID currentTask; // currently retrieving output

    MapOutputInputStream(Configuration conf, TaskID reduceId,
            Counter inputCounter, SecretKey jobTokenSecret,
            List<MapOutputSplit> splits) throws IOException {
        if (conf.getBoolean(JobContext.MAP_OUTPUT_COMPRESS, false)) {
            Class<? extends CompressionCodec> codecClass = getMapOutputCompressorClass(
                    conf, DefaultCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            decompressor = CodecPool.getDecompressor(codec);
        } else {
            codec = null;
            decompressor = null;
        }

        this.inputCounter = inputCounter;
        this.jobTokenSecret = jobTokenSecret;
        this.reduceTaskId = reduceId;

        int totalBufSz = conf.getInt("skewtune.map.io.inputbuf", 4 * 1024 * 1024); // 4 MB
        PACKET_SIZE = conf.getInt("skewtune.map.io.packetsize", 128 * 1024); // 128KB

        final int numBuf = totalBufSz / PACKET_SIZE;
        buffers = new ByteBuffer[numBuf];
        for (int i = 0; i < numBuf; ++i ) {
            buffers[i] = ByteBuffer.allocate(PACKET_SIZE);
        }
        this.splits = splits;

        this.q = new ArrayBlockingQueue<ByteBuffer>(numBuf-2); // producer and consumer may keep one buffer at their hands
        this.fetcher = new Fetcher(conf, reduceId);
        this.fetcher.start();

        progress = new Progress();
        progress.addPhases(splits.size());
    }
    
    public float getProgress() {
        return progress.get();
    }

    @Override
    public int available() throws IOException {
        // FIXME later support it
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
        fetcher.interrupt(); // fetcher is daemon. just leave now.
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
//        if ( LOG.isDebugEnabled() ) {
//            LOG.debug(String.format("read(%s, %d, %d)",b,off,len));
//        }
        
        if ( buf == null ) {
            if ( ! waitNextBuffer() ) {
                return -1;
            }
        }
        
        int remaining = len;
        while (remaining > 0) {
            if ( ! buf.hasRemaining() ) {
                if (closed)
                    return -1; // we are done!

                // wait until buffer is filled.
                if (!waitNextBuffer()) {
                    closed = true;
                    if (remaining == len) {
                        return -1;
                    } else {
                        int rbytes = len - remaining;
                        inputCounter.increment(rbytes);
                        bytesRead += rbytes;
                        return rbytes;
                    }
                }
            }
            int toRead = Math.min(buf.remaining(), remaining);
            buf.get(b, off, toRead);
            remaining -= toRead;
            off += toRead;
        }
        final int rbytes = len - remaining;
        bytesRead += rbytes;
        inputCounter.increment(rbytes);
        return rbytes;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long skip(long n) throws IOException {
        if ( buf == null ) {
            if ( ! waitNextBuffer() ) {
                return -1;
            }
        }
        long toSkip = n - buf.remaining();
        while (toSkip > 0) {
            if ( ! waitNextBuffer() ) {
                // we got ZERO bytes. nothing to skip.
                closed = true;
                long sz = n - toSkip;
                inputCounter.increment(sz);
                bytesRead += sz;
                return n - toSkip; // skipped bytes
            }
            toSkip -= buf.remaining(); // discard entire content
        }
        inputCounter.increment(n);
        bytesRead += n;
        return n;
    }

    @Override
    public int read() throws IOException {
        if ( buf == null || ! buf.hasRemaining() ) {
            if ( ! waitNextBuffer() ) {
                if ( LOG.isDebugEnabled() ) {
                    LOG.debug("read() : EOF");
                }
                return -1; // EOF
            }
        }
        inputCounter.increment(1);
        ++bytesRead;
        int b = (int) (buf.get() & 0x0ff);
//        if ( LOG.isDebugEnabled() ) {
//            LOG.debug("read() : "+b);
//            LOG.debug(buf.toString());
//        }
        return b;
    }

    private boolean waitNextBuffer() throws IOException {
        if (ioException != null)
            throw ioException;
        try {
            buf = q.take();
//            if ( LOG.isDebugEnabled() ) {
//                LOG.debug("got new data buffer. "+buf.remaining()+" bytes");
//            }
            return buf.remaining() > 0; // otherwise, it's EOF
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public long getPosition() {
        return bytesRead;
    }

    public boolean hasMore() {
        return more.get() && bytesRead < totalBytes.get();
    }
    
    public String getCurrentHost() { return currentHost; }
    public TaskAttemptID getCurrentTask() { return currentTask; }

    /**
     * From org.apache.hadoop.mapred.JobConf
     * 
     * Get the {@link CompressionCodec} for compressing the map outputs.
     * 
     * @param defaultValue
     *            the {@link CompressionCodec} to return if not set
     * @return the {@link CompressionCodec} class that should be used to
     *         compress the map outputs.
     * @throws IllegalArgumentException
     *             if the class was specified, but not found
     */
    Class<? extends CompressionCodec> getMapOutputCompressorClass(
            Configuration conf, Class<? extends CompressionCodec> defaultValue) {
        Class<? extends CompressionCodec> codecClass = defaultValue;
        String name = conf.get(JobContext.MAP_OUTPUT_COMPRESS_CODEC);
        if (name != null) {
            try {
                codecClass = conf.getClassByName(name).asSubclass(
                        CompressionCodec.class);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Compression codec " + name
                        + " was not found.", e);
            }
        }
        return codecClass;
    }

    class Fetcher extends Thread {

        /** Number of ms before timing out a copy */
        private static final int DEFAULT_STALLED_COPY_TIMEOUT = 3 * 60 * 1000;

        /** Basic/unit connection timeout (in milliseconds) */
        private final static int UNIT_CONNECT_TIMEOUT = 60 * 1000;

        /* Default read timeout (in milliseconds) */
        private final static int DEFAULT_READ_TIMEOUT = 3 * 60 * 1000;

        private final int reduce;

        private final int connectionTimeout;

        private final int readTimeout;

        Fetcher(Configuration conf, TaskID reduceId) throws IOException {
            this.reduce = reduceId.getId();
            this.connectionTimeout = conf.getInt(MRJobConfig.SHUFFLE_CONNECT_TIMEOUT,DEFAULT_STALLED_COPY_TIMEOUT);
            this.readTimeout = conf.getInt(MRJobConfig.SHUFFLE_READ_TIMEOUT,DEFAULT_READ_TIMEOUT);

            setName("SkewTune MapOutput Fetcher");
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                for (MapOutputSplit split : splits) {
                    // each split is associated with a single host
                    currentHost = split.getHost();
                    copyFromHost(split);
                    progress.startNextPhase();
                }
                more.set(false); // mark it as false
                q.put(ByteBuffer.allocate(0)); // EOF buffer
            } catch (InterruptedException ie) {
                return;
            } catch (Throwable t) {
                ioException = new IOException(t);
            }
        }

        /**
         * The crux of the matter...
         * 
         * @param host
         *            {@link MapHost} from which we need to shuffle available
         *            map-outputs.
         * @throws InterruptedException
         */
        private void copyFromHost(final MapOutputSplit split)
                throws IOException, InterruptedException {
            // Get completed maps on 'host'
            List<TaskAttemptID> maps = split.getMaps();

            // Sanity check to catch hosts with only 'OBSOLETE' maps,
            // especially at the tail of large jobs
            if (maps.size() == 0) {
                return;
            }

            // List of maps to be fetched yet
            Set<TaskAttemptID> remaining = new HashSet<TaskAttemptID>(maps);

            if (LOG.isDebugEnabled()) {
                LOG.debug("SkewTune Output Fetcher going to fetch from "
                        + split.getHost());
                for ( TaskAttemptID tmp : remaining ) {
                    LOG.debug(tmp);
                }
            }


            // Construct the url and connect
            DataInputStream input;
            boolean connectSucceeded = false;

            try {
                URL url = getMapOutputURL(split);
                URLConnection connection = url.openConnection();

                // generate hash of the url
                String msgToEncode = SecureShuffleUtils.buildMsgFrom(url);
                String encHash = SecureShuffleUtils.hashFromString(msgToEncode,
                        jobTokenSecret);

                // put url hash into http header
                connection.addRequestProperty(
                        SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);
                // set the read timeout
                connection.setReadTimeout(readTimeout);
                connect(connection, connectionTimeout);
                connectSucceeded = true;
                input = new DataInputStream(connection.getInputStream());

                // get the replyHash which is HMac of the encHash we sent to the
                // server
                String replyHash = connection
                        .getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH);
                if (replyHash == null) {
                    throw new IOException(
                            "security validation of TT Map output failed");
                }
                LOG.debug("url=" + msgToEncode + ";encHash=" + encHash
                        + ";replyHash=" + replyHash);
                // verify that replyHash is HMac of encHash
                SecureShuffleUtils.verifyReply(replyHash, encHash,
                        jobTokenSecret);
                LOG.info("for url=" + msgToEncode
                        + " sent hash and receievd reply");
            } catch (IOException ie) {
                ioException = ie;
                LOG.warn("Failed to connect to " + split.getHost() + " with "
                        + remaining.size() + " map outputs", ie);
                return;
            }

            // Loop through available map-outputs and fetch them
            // On any error, good becomes false and we exit after putting back
            // the remaining maps to the yet_to_be_fetched list
            boolean good = true;
            while (!remaining.isEmpty() && good) {
                good = copyMapOutput(split, input, remaining);
            }

            IOUtils.cleanup(LOG, input);

            // Sanity check
            if (good && !remaining.isEmpty()) {
                throw new IOException(
                        "server didn't return all expected map outputs: "
                                + remaining.size() + " left.");
            }
        }

        private boolean copyMapOutput(final MapOutputSplit split,
                DataInputStream input, Set<TaskAttemptID> remaining)
                throws InterruptedException {
            TaskAttemptID mapId = null;
            long decompressedLength = -1;
            long compressedLength = -1;

            try {
                long startTime = System.currentTimeMillis();
                int forReduce = -1;
                // Read the shuffle header
                try {
                    ShuffleHeader header = new ShuffleHeader();
                    header.readFields(input);
                    mapId = TaskAttemptID.forName(header.mapId);
                    compressedLength = header.compressedLength;
                    decompressedLength = header.uncompressedLength;
                    forReduce = header.forReduce;
                } catch (IllegalArgumentException e) {
                    LOG.warn("Invalid map id ", e);
                    return false;
                }

                // Do some basic sanity verification
                if (!verifySanity(compressedLength, decompressedLength,
                        forReduce, remaining, mapId)) {
                    return false;
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("header: " + mapId + ", len: " + compressedLength
                            + ", decomp len: " + decompressedLength);
                }

                // Go!
                LOG.info("about to shuffle output of map " + mapId
                        + " decomp: " + decompressedLength + " len: "
                        + compressedLength);

                currentTask = mapId;
                
                totalBytes.addAndGet(decompressedLength);

                shuffleToMemory(mapId, input, (int) decompressedLength, (int) compressedLength);

                long endTime = System.currentTimeMillis();
                
                // Note successful shuffle
                remaining.remove(mapId);
                
                LOG.info("shuffle complete for map " + mapId + ": " + (endTime - startTime)/1000.0 + "s");

                return true;
            } catch (IOException ioe) {
                ioException = ioe;
                if (mapId == null) {
                    LOG.info("fetcher failed to read map header" + mapId
                            + " decomp: " + decompressedLength + ", "
                            + compressedLength, ioe);
                    return false;
                }
                LOG.info("Failed to shuffle output of " + mapId + " from " + split.getHost(), ioe);
                return false;
            }
        }

        /**
         * Do some basic verification on the input received -- Being defensive
         * 
         * @param compressedLength
         * @param decompressedLength
         * @param forReduce
         * @param remaining
         * @param mapId
         * @return true/false, based on if the verification succeeded or not
         */
        private boolean verifySanity(long compressedLength,
                long decompressedLength, int forReduce,
                Set<TaskAttemptID> remaining, TaskAttemptID mapId) {
            if (compressedLength < 0 || decompressedLength < 0) {
                LOG.warn(getName()
                        + " invalid lengths in map output header: id: " + mapId
                        + " len: " + compressedLength + ", decomp len: "
                        + decompressedLength);
                return false;
            }

            if (forReduce != reduce) {
                LOG.warn(getName() + " data for the wrong reduce map: " + mapId
                        + " len: " + compressedLength + " decomp len: "
                        + decompressedLength + " for reduce " + forReduce);
                return false;
            }

            // Sanity check
            if (!remaining.contains(mapId)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("content of remaining set:");
                    for ( TaskAttemptID tmp : remaining ) {
                        LOG.debug(tmp);
                    }
                }
                LOG.warn("Invalid map-output! Received output for " + mapId);
                return false;
            }

            return true;
        }
        
        private URI getBaseURI(MapOutputSplit split) {
            String url = split.getBaseUrl();
            StringBuffer baseUrl = new StringBuffer(url);
            if (!url.endsWith("/")) {
              baseUrl.append("/");
            }
            baseUrl.append("mapOutput?job=");
            baseUrl.append(reduceTaskId.getJobID());
            baseUrl.append("&reduce=");
            baseUrl.append(reduceTaskId.getId());
            baseUrl.append("&map=");
            URI u = URI.create(baseUrl.toString());
            return u;
          }

        /**
         * Create the map-output-url. This will contain all the map ids
         * separated by commas
         * 
         * @param host
         * @param maps
         * @return
         * @throws MalformedURLException
         */
        private URL getMapOutputURL(MapOutputSplit split)
                throws MalformedURLException {
            // Get the base url
            StringBuffer url = new StringBuffer(getBaseURI(split).toString());

            boolean first = true;
            for (TaskAttemptID mapId : split) {
                if (!first) {
                    url.append(",");
                }
                url.append(mapId);
                first = false;
            }

            LOG.debug("MapOutput URL for " + split.getHost() + " -> "
                    + url.toString());
            return new URL(url.toString());
        }

        /**
         * The connection establishment is attempted multiple times and is given
         * up only on the last failure. Instead of connecting with a timeout of
         * X, we try connecting with a timeout of x < X but multiple times.
         */
        private void connect(URLConnection connection, int connectionTimeout)
                throws IOException {
            int unit = 0;
            if (connectionTimeout < 0) {
                throw new IOException("Invalid timeout " + "[timeout = "
                        + connectionTimeout + " ms]");
            } else if (connectionTimeout > 0) {
                unit = Math.min(UNIT_CONNECT_TIMEOUT, connectionTimeout);
            }
            // set the connect timeout to the unit-connect-timeout
            connection.setConnectTimeout(unit);
            while (true) {
                try {
                    connection.connect();
                    break;
                } catch (IOException ioe) {
                    // update the total remaining connect-timeout
                    connectionTimeout -= unit;

                    // throw an exception if we have waited for timeout amount
                    // of
                    // time note that the updated value if timeout is used here
                    if (connectionTimeout == 0) {
                        throw ioe;
                    }

                    // reset the connect timeout for the last try
                    if (connectionTimeout < unit) {
                        unit = connectionTimeout;
                        // reset the connect time out for the final connect
                        connection.setConnectTimeout(unit);
                    }
                }
            }
        }

        private void shuffleToMemory(TaskAttemptID mapId, InputStream input,
                int decompressedLength, int compressedLength)
                throws IOException, InterruptedException {
            IFileInputStream checksumIn = new IFileInputStream(input,compressedLength);

            input = checksumIn;

            // Are map-outputs compressed?
            if (codec != null) {
                decompressor.reset();
                input = codec.createInputStream(input, decompressor);
            }

            // Copy map-output into an in-memory buffer
            try {
                int remain = decompressedLength;
                while (remain > 0) {
                    int readLen = Math.min(PACKET_SIZE, remain);
                    ByteBuffer buf = buffers[(nextBuf++) % buffers.length];
//                    if ( LOG.isDebugEnabled() ) {
//                        LOG.debug(String.format("remain = %d; read = %d; buf.offset = %d; %s",remain,readLen,buf.arrayOffset(),buf));
//                    }
                    IOUtils.readFully(input, buf.array(), buf.arrayOffset(), readLen);
                    remain -= readLen;
                    buf.clear().limit(readLen);
                    // put to queue
                    q.put(buf);
                    
                    progress.set(1.0f - ((float)remain / decompressedLength));
                }
                LOG.info("Read " + decompressedLength + " bytes from map-output for " + mapId);
            } catch (IOException ioe) {
                // Close the streams
                IOUtils.cleanup(LOG, input);

                // Re-throw
                throw ioe;
            }
        }
    }
}
