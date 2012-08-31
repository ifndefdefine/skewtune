package skewtune.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapred.TaskAttemptID;

import skewtune.mapreduce.protocol.MapOutputUpdates;
import skewtune.mapreduce.protocol.STTaskStatus;
import skewtune.mapreduce.protocol.SkewTuneTaskUmbilicalProtocol;

public class STTaskUmbilicalClient implements SkewTuneTaskUmbilicalProtocol {

    @Override
    public long getProtocolVersion(String arg0, long arg1) throws IOException {
        return 0;
    }

    @Override
    public int init(TaskAttemptID taskId, int nMap, int nReduce)
            throws IOException {
        return 0;
    }

    @Override
    public int done(TaskAttemptID taskId) throws IOException {
        return 0;
    }

    @Override
    public int ping(TaskAttemptID taskId) throws IOException {
        return 0;
    }

    @Override
    public int statusUpdate(TaskAttemptID taskid, STTaskStatus status)
            throws IOException {
        return 0;
    }

    @Override
    public MapOutputUpdates getCompltedMapOutput(TaskAttemptID taskid, int from, int from2)
            throws IOException {
        return null;
    }

    @Override
    public void setSplitInfo(TaskAttemptID taskid, byte[] data) throws IOException {
    }
}
