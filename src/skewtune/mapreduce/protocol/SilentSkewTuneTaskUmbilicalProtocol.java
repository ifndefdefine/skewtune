package skewtune.mapreduce.protocol;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TaskAttemptID;

import skewtune.mapreduce.STTaskTracker;

/**
 * Wrap the protocol so that when it fails, fail silently.
 * 
 * @author yongchul
 *
 */
public class SilentSkewTuneTaskUmbilicalProtocol implements
        SkewTuneTaskUmbilicalProtocol {
    public static final Log LOG = LogFactory.getLog(SilentSkewTuneTaskUmbilicalProtocol.class);
    private final SkewTuneTaskUmbilicalProtocol delegate;
    private final AtomicBoolean ok;
    private IOException lastException;
    
    public SilentSkewTuneTaskUmbilicalProtocol(SkewTuneTaskUmbilicalProtocol org) {
        delegate = org;
        ok = new AtomicBoolean(delegate != null);
    }
    
    @Override
    public long getProtocolVersion(String arg0, long arg1) throws IOException {
        if ( ok.get() ) {
            try {
                return delegate.getProtocolVersion(arg0, arg1);
            } catch ( IOException e ) {
                LOG.error(e);
                lastException = e;
                ok.set(false);
            }
        }
        return Long.MAX_VALUE;
    }

    /**
     * delegate initialize call. this method rethrow the exception if there was a network exception.
     */
    @Override
    public int init(TaskAttemptID taskId, int nMap, int nReduce)
            throws IOException {
        if ( ok.get() ) {
            try {
                return delegate.init(taskId, nMap, nReduce);
            } catch ( IOException e ) {
                LOG.error(e);
                lastException = e;
                ok.set(false);
                throw e;
            }
        }
        return RESP_CANCEL;
    }

    @Override
    public int done(TaskAttemptID taskId) throws IOException {
//        if ( ok.get() ) {
            try {
                return delegate.done(taskId);
            } catch ( IOException e ) {
                LOG.error("failed to call done",e);
                lastException = e;
                ok.set(false);
            }
//        }
        return RESP_CANCEL;
    }

    @Override
    public int ping(TaskAttemptID taskId) throws IOException {
        if ( ok.get() ) {
            try {
                return delegate.ping(taskId);
            } catch ( IOException e ) {
                LOG.error(e);
                lastException = e;
                ok.set(false);
            }
        }
        return RESP_CANCEL;
    }

    @Override
    public int statusUpdate(TaskAttemptID taskId, SRTaskStatus status)
            throws IOException {
//        if ( ok.get() ) {
            try {
                return delegate.statusUpdate(taskId,status);
            } catch ( IOException e ) {
                LOG.error(e);
                lastException = e;
                ok.set(false);
            }
//        }
        return RESP_CANCEL;
    }

    @Override
    public MapOutputUpdates getCompltedMapOutput(TaskAttemptID taskId, int from, int fromTakeover)
            throws IOException {
        if ( ok.get() ) {
            try {
                return delegate.getCompltedMapOutput(taskId, from, fromTakeover);
            } catch ( IOException e ) {
                LOG.error(e);
                lastException = e;
                ok.set(false);
            }
        }
        return MapOutputUpdates.EMPTY_UPDATE;
    }
    
    public IOException getLastException() { return lastException; }
    public boolean isAlive() { return ok.get(); }

    @Override
    public void setSplitInfo(TaskAttemptID taskid, byte[] data)
            throws IOException {
    }
}
