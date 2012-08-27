package skewtune.mapreduce;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.commons.httpclient.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SpeculationEventNotifier;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import skewtune.mapreduce.protocol.SkewTuneClientProtocol;
import skewtune.mapreduce.server.jobtracker.JTConfig;
import skewtune.mapreduce.util.ConfigUtil;

public class SkewTuneJob implements MRJobConfig, SkewTuneJobConfig {
    private final SkewTuneClientProtocol client;

    private final Cluster cluster;
    private final Job job;
    private final String notifyUrl; // cache
    private final String speculateUrl;
    private final String trackerHttp;
    private final boolean useNewApi;
    private ClusterMetrics clusterMetrics;
    
    private static final Log LOG = LogFactory.getLog(SkewTuneJob.class);

    static {
        ConfigUtil.loadResources();
    }
    
    private SkewTuneJob(JobConf conf) throws IOException, InterruptedException {
        this.cluster = new Cluster(conf);
        this.clusterMetrics = cluster.getClusterStatus();
        this.job = Job.getInstance(cluster,conf);
        client = createClient(conf);
        trackerHttp = client.getHttpAddress();
        notifyUrl = client.getCompletionUrl();
        speculateUrl = client.getSpeculationEventUrl();
        useNewApi = false;
    }

    private SkewTuneJob(Cluster cluster,Job job) throws IOException, InterruptedException {
        this.job = job;
        this.cluster = cluster;
        this.clusterMetrics = cluster.getClusterStatus();
        client = createClient(job.getConfiguration());
        trackerHttp = client.getHttpAddress();
        notifyUrl = client.getCompletionUrl();
        speculateUrl = client.getSpeculationEventUrl();
        useNewApi = true;
    }

    private SkewTuneClientProtocol createRPCProxy(InetSocketAddress addr,
            Configuration conf) throws IOException {
        return (SkewTuneClientProtocol) RPC
                .getProxy(SkewTuneClientProtocol.class,
                        SkewTuneClientProtocol.versionID, addr,
                        UserGroupInformation.getCurrentUser(), conf, NetUtils
                                .getSocketFactory(conf,
                                        SkewTuneClientProtocol.class));
    }

    private SkewTuneClientProtocol createClient(Configuration conf)
            throws IOException {
        SkewTuneClientProtocol client;
        String tracker = conf.get(JTConfig.JT_IPC_ADDRESS, "local");
        if ("local".equals(tracker)) {
            // conf.setInt("mapreduce.job.maps", 1);
            // client = new LocalJobRunner(conf);
            client = null;
        } else {
            client = createRPCProxy(STJobTracker.getAddress(conf), conf);
        }
        return client;
    }

    public static SkewTuneJob getInstance(Cluster cluster) throws IOException, InterruptedException {
        return new SkewTuneJob(cluster,Job.getInstance(cluster));
    }

    public static SkewTuneJob getInstance(Cluster cluster,Job job) throws IOException, InterruptedException {
        return new SkewTuneJob(cluster,job);
    }
    
    public static SkewTuneJob getInstance(Job job) throws IOException, InterruptedException {
        return new SkewTuneJob(new Cluster(job.getConfiguration()),job);
    }

    public static SkewTuneJob getInstance(Cluster cluster, Configuration conf)
            throws IOException, InterruptedException {
        return new SkewTuneJob(cluster,Job.getInstance(cluster, conf));
    }
    
    public static SkewTuneJob getInstance(JobConf conf) throws IOException, InterruptedException {
        return new SkewTuneJob(conf);
    }

    public Job getJob() {
        return job;
    }
    
    public Cluster getCluster() {
        return cluster;
    }

    public boolean getNewApi() {
        return useNewApi;
    }
    
    /**
     * Define secondary partitioner. SkewTune framework will use the secondary
     * partitioner when splitting a skewed reduce. If not specified, the
     * framework assumes a hash partitioning scheme and tell the partitioner
     * that there are (2^i)*N reducers so that the original reduce input is
     * further scattered across reactive reduces.
     * 
     * @param cls
     *            partitioner class
     */
    public void setSecondaryPartitionerClass(
            Class<? extends Partitioner<?, ?>> cls) {
        job.getConfiguration().setClass(
                REACTIVE_PARTITIONER_CLASS_ATTR, cls,
                Partitioner.class);
    }
    
    @SuppressWarnings("deprecation")
    public void submit() throws IOException, InterruptedException,
            ClassNotFoundException {
        Configuration conf = job.getConfiguration();
        
        System.out.println("num reduces = "+job.getNumReduceTasks());
        
        if ( job.getNumReduceTasks() > 0 ) {
            // enforce partitioner
            boolean hasPartitioner = true;
            try {
                Class<?> cls = job.getPartitionerClass();
                hasPartitioner = cls != null;
                System.out.println("Partitioner class = "+cls.toString());
            } catch ( ClassNotFoundException ignore ) {
                hasPartitioner = false;
            }
            
            if ( ! hasPartitioner ) {
                LOG.warn("setting up default hash partitioner since there is no partitioner class");
                job.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);
            }
        }
        
        // FIXME is this right?
        job.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
        
        // first spool configuration?
        conf.setBoolean(ENABLE_SKEWTUNE, true);
        conf.setBooleanIfUnset(ENABLE_SKEWTUNE_MAP, true);
        conf.setBooleanIfUnset(ENABLE_SKEWTUNE_REDUCE, true);
        
        String notifyUrl = conf.get(END_NOTIFICATION_URL, "");
        if (notifyUrl.length() > 0) {
            conf.set(ORIGINAL_END_NOTIFICATION_URL,notifyUrl);
        }
        conf.set(END_NOTIFICATION_URL, this.notifyUrl);
        conf.set(SKEWTUNE_JT_HTTP_ATTR,trackerHttp);
        
//        if ( conf.getBoolean(JobContext.REDUCE_SPECULATIVE, false)
//                || conf.getBoolean(JobContext.MAP_SPECULATIVE, false) ) {
//            conf.set(SpeculationEventNotifier.SPECULATION_EVENT_URI, this.speculateUrl);
//        }
        
        URI uri = new URI(this.notifyUrl,false);
        conf.set(SkewTuneJobConfig.MAP_INPUT_MONITOR_URL_ATTR, String.format("%s://%s:%d/skew", uri.getScheme(), uri.getHost(), uri.getPort()));
        conf.setInt(SkewTuneJobConfig.MAP_NUM_SPLITS_ATTR, clusterMetrics.getMapSlotCapacity()-1); // by default, cluster capacity

        // FIXME check input format and input split and check the jar file.
        // later these jar files must be cached to the coordinator to split the
        // map. -- or
        
        if ( ! useNewApi ) {
            conf.setBooleanIfUnset("mapred.mapper.new-api", false);
            conf.setBooleanIfUnset("mapred.reducer.new-api", false);
        }

        job.submit();

        if ( LOG.isInfoEnabled() ) {
            LOG.info("job submitted: "+job.getJobID());
        }
        
        // now we need to cleanup the job configuration a bit
        // first, clear all temporary cache information
//        conf.set("tmpfiles", "");
//        conf.set("tmpjars", "");
//        conf.set("tmparchives", "");
        
        // finally, add job jar to classpath. manually add the job file to the cache
        Path jobSubmitDir = new Path(conf.get("mapreduce.job.dir"));
        Path jarFile = JobSubmissionFiles.getJobJar(jobSubmitDir);
        
        if ( LOG.isInfoEnabled() ) {
            LOG.info("adding jar "+jarFile+" to distributed cache");
        }
        
        // FIXME
        DistributedCache.addFileToClassPath(jarFile, conf);
        
        // now add this to tracker
        client.submitJob(org.apache.hadoop.mapred.JobID.downgrade(job.getJobID()), new JobConfTemplate(job.getConfiguration()));
        
        LOG.info("submitted to SkewTune jobtracker");
    }

    public void killJob() throws IOException, InterruptedException {
        client.killJob(org.apache.hadoop.mapred.JobID.downgrade(job.getJobID()));
        job.killJob();
    }
    
    JobID splitTaskInternal(TaskID taskid,int n) throws IOException, InterruptedException {
        return client.splitTask(org.apache.hadoop.mapred.TaskID.downgrade(taskid), n);
    }
    
    public JobID splitTask(TaskID taskid,int n) throws IOException, InterruptedException {
        if ( job.isComplete() ) {
            throw new IllegalStateException("already completed");
        }
        return splitTaskInternal(taskid,n);
    }
    
    public JobID splitMapTask(int partition,int n) throws IOException, InterruptedException {
        return splitTask(new TaskID(job.getJobID(),TaskType.MAP,partition),n);
    }

    public JobID splitReduceTask(int partition,int n) throws IOException, InterruptedException {
        return splitTask(new TaskID(job.getJobID(),TaskType.REDUCE,partition),n);
    }

    public boolean waitForCompletion(boolean verbose) throws IOException, InterruptedException, ClassNotFoundException {
        return job.waitForCompletion(verbose);
    }
    
    public boolean monitorAndPrintJob() throws IOException,InterruptedException {
        return job.monitorAndPrintJob();
    }
    
    public static final class JobConfTemplate extends Configuration {
        public JobConfTemplate() {
            super(false); // this is only used to deserialize at JobTracker side.
        }
        
        private JobConfTemplate(Configuration conf) {
            super(conf);
            // now trim unnecessary attributes
            trim();
        }
        
        void trim() {
            Properties props = super.getProps();
            props.remove("tmpfiles");
            props.remove("tmpjars");
            props.remove("tmparchives");
            props.remove(JAR);
        }
    }
}
