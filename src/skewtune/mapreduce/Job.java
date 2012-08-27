package skewtune.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * utility to control skew reduce job
 * @author yongchul
 */
public class Job extends Configured implements Tool {

    public Job() {}

    public Job(Configuration conf) {
        super(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
        // first argument is always command
        if ( args.length < 2 ) {
            System.err.println("usage: skewred command [args...]");
            return -1;
        }
        
        int i = 0;
        String cmd = args[i++];
        
        if ( "split".equals(cmd) ) {
            return splitTask(args);
        } else {
            // unrecognized command
            System.err.println("Unrecognized command: "+cmd);
        }
        
        return 0;
    }

    private int splitTask(String[] args) throws IOException, InterruptedException {
        // args[1] must be task id
        // args[2] must be the number of partitions
        
        if ( args.length != 2 && args.length != 3 ) {
            System.err.println("usage: split <taskid> [num partitions]");
            return -1;
        }
        
        TaskID taskid = TaskID.forName(args[1]);
        Cluster cluster = new Cluster(getConf());
        ClusterMetrics clusterStatus = cluster.getClusterStatus();

        int n;
        if ( args.length == 3 ) {
            n = Integer.parseInt(args[2]);
        } else {
            n = ( clusterStatus.getMapSlotCapacity() - clusterStatus.getOccupiedMapSlots() ) >> 1;
        }
        if ( n < 2 ) {
            System.err.println("force set number of partitions to 2: given "+n);
            n = 2;
        }
        
        SkewTuneJob srjob = SkewTuneJob.getInstance(cluster,getConf());

        JobID jobid = srjob.splitTaskInternal(taskid, n);
        
        System.out.println("Splitted "+taskid+" as a reactive job "+jobid);
        
        return 0;
    }

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Job(),args);
        System.exit(rc);
    }
}
