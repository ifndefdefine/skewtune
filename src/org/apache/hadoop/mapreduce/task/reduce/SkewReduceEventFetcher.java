package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import skewtune.mapreduce.protocol.MapOutputUpdates;
import skewtune.mapreduce.protocol.NewMapOutput;
import skewtune.mapreduce.protocol.SkewTuneTaskUmbilicalProtocol;
import skewtune.mapreduce.protocol.TaskStatusEvent;

class SkewReduceEventFetcher<K, V> extends Thread {
    static final long SLEEP_TIME = 1000;

//    private static final int MAX_EVENTS_TO_FETCH = 10000;
//    private static final int MAX_RETRIES = 10;

    static final int RETRY_PERIOD = 5000;

    static final Log LOG = LogFactory.getLog(SkewReduceEventFetcher.class);

    final TaskAttemptID reduce;

    final ShuffleScheduler<K, V> scheduler;

    int fromEventId = 0;

    ExceptionReporter exceptionReporter = null;

    // SKEWREDUCE
    private final SkewTuneTaskUmbilicalProtocol srumbilical;
    int fromReactiveEventId;
    int fromTakeoverEventId;
    final Random random = new Random();
    
    public SkewReduceEventFetcher(TaskAttemptID reduce, SkewTuneTaskUmbilicalProtocol umbilical,
            ShuffleScheduler<K, V> scheduler, ExceptionReporter reporter) {
        setName("SkewReduce EventFetcher for fetching Map Completion Events");
        setDaemon(true);
        this.reduce = reduce;
        this.srumbilical = umbilical;
        this.scheduler = scheduler;
        exceptionReporter = reporter;
    }
    
    private int getReactiveMapCompletionEvents() throws IOException {
        MapOutputUpdates update = srumbilical.getCompltedMapOutput((org.apache.hadoop.mapred.TaskAttemptID)reduce, fromReactiveEventId, fromTakeoverEventId);
        NewMapOutput[] events = update.getCompletedMaps();
        TaskStatusEvent.Internal[] takeovers = update.getTakeoverMaps();
        if ( LOG.isDebugEnabled() ) {
            if ( events.length > 0 ) {
                LOG.debug("Got " + events.length + " reactive map completion events from " + fromReactiveEventId);
            }
            if ( takeovers.length > 0 ) {
                LOG.debug("Got " + takeovers.length + " map state events from "+ fromTakeoverEventId);
            }
        }

        // Update the last seen event ID
        fromReactiveEventId += events.length;
        fromTakeoverEventId += takeovers.length;
        if ( takeovers.length > 0 ) {
            scheduler.updateTaskState(takeovers);
        }
        
        for ( NewMapOutput event : events ) {
            // add to scheduler.
            ReactiveMapOutput<K,V> rmapout = new ReactiveMapOutput<K,V>(reduce,event,random);
            scheduler.addKnownReactiveMapOutput(rmapout);
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("adding a new reactive map ouptut for "+rmapout.getTaskID());
            }
        }
        return events.length;
    }
    
    @Override
    public void run() {
        try {
            while ( true ) {
                try {
                    int numNewMaps = getReactiveMapCompletionEvents();
                    if (numNewMaps > 0) {
                        LOG.info(reduce + ": " + "Got " + numNewMaps + " new reactive map-outputs");
                    }
                    if ( LOG.isTraceEnabled() )
                        LOG.trace("GetMapEventsThread about to sleep for " + SLEEP_TIME);
                    Thread.sleep(SLEEP_TIME);
                } catch ( IOException ie) {
                    LOG.info("Exception in getting reactive events", ie);
                    // sleep for a bit
                    Thread.sleep(RETRY_PERIOD);
                }
            }
        } catch ( InterruptedException e ) {
            return;
        } catch ( Throwable t) {
            exceptionReporter.reportException(t);
            return;
        }
    }
}