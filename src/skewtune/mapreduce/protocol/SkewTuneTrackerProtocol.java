package skewtune.mapreduce.protocol;

import java.io.IOException;

public interface SkewTuneTrackerProtocol {
    public static final long versionID = 0;
    public HeartbeatResponse heartbeat(TaskTrackerStatus status, boolean justStarted,
            boolean justInited, short responseId) throws IOException,
            InterruptedException;
}
