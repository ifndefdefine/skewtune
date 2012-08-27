package skewtune.mapreduce.protocol;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;

public interface SkewTuneClientProtocol {
    public static final long versionID = 0;
    
    public String getSpeculationEventUrl() throws IOException, InterruptedException;
    public String getCompletionUrl() throws IOException, InterruptedException;
    public String getHttpAddress() throws IOException, InterruptedException;

    /**
     * Submit a Job for execution.  Returns the latest profile for
     * that job.
     */
    public void submitJob(JobID jobId, Configuration conf) throws IOException, InterruptedException;
    
    /**
     * Kill the indicated job
     */
    public void killJob(JobID jobid) throws IOException, InterruptedException;
    
    public JobID splitTask(TaskID taskid,int n) throws IOException, InterruptedException;


    Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
            throws IOException, InterruptedException;

    long renewDelegationToken(Token<DelegationTokenIdentifier> token)
            throws IOException, InterruptedException;

    void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
            throws IOException, InterruptedException;
    
}
