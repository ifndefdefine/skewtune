package skewtune.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

public class PlannedJob {
    public static final Log LOG = LogFactory.getLog(PlannedJob.class);

    private final JobID jobid;
    private final TaskType taskType;
    private final long plannedAt;
    private final double[] expectedTime; // numMaps + numReduces
    
    private int remains;
    
    public PlannedJob(JobInProgress jip,long at,double[] times) {
        jobid = jip.getJobID();
        taskType = jip.getParentTaskID().getTaskType();
        
        plannedAt = at;
        expectedTime = times;
        
        remains = taskType == TaskType.MAP ? jip.getNumMapTasks() : jip.getNumReduceTasks();
    }
    
    public synchronized int fillCompletionTime(TaskType type,long now,double[] slots,int off) {
        if ( taskType != type ) return off; // nothing change

        for ( int i = 0; i < expectedTime.length && off > 0; ++i ) {
            double t = (plannedAt - now)*0.001 + expectedTime[i];
            if ( t > 0. ) slots[--off] = t;
        }
        return off;
    }
    
    public boolean remove(TaskAttemptID taskid) {
        return remove(taskid.getTaskID());
    }
    
    public synchronized boolean remove(TaskID taskid) {
        if ( taskid.getTaskType() != taskType ) return false;
        
        if ( expectedTime[taskid.getId()] < 0. ) {
            LOG.error(taskid + " has been removed already!");
        } else {
            --remains;
        }
        
        return remains == 0;
    }
    
    @Override
    public String toString() {
        return jobid.toString() + " planned at " + plannedAt + " expected time = "+ expectedTime;
    }
}
