package org.apache.hadoop.mapred;

import java.io.IOException;

public interface MergerProgressListener {
    // keep track of merge
    
    public void update(long bytes,long total) throws IOException;

    /**
     * signal starting final merge.
     * @param total number of bytes left to final merge
     * @param bytes cumulative bytes processed so far
     */
    public void beginFinalMerge(long total, long bytes);
    
    public void setProcessedBytesByKey(long bytes) throws IOException;
    
    /**
     * capturing how many bytes have been processed through this merge
     * @param bytes cumulative bytes processed so far
     */
    public void setProcessedBytesByValue(long bytes) throws IOException;
    
    public void endMerge();
}
