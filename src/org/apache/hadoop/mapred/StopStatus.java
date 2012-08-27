package org.apache.hadoop.mapred;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public enum StopStatus {
    STOPPED,
    CANNOT_STOP,
    RUNNING,
    STOPPING;
    
    public static Future future(StopStatus status) {
        return new Future(status);
    }
    
    public static class Future implements java.util.concurrent.Future<StopStatus> {
        final StopStatus status;
        public Future(StopStatus s) {
            status = s;
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public StopStatus get() throws InterruptedException, ExecutionException {
            return status;
        }

        @Override
        public StopStatus get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException,
                TimeoutException {
            return status;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }
    }
}