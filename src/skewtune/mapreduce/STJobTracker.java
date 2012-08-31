/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package skewtune.mapreduce;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.util.MRAsyncDiskService;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserToGroupMappingsProtocol;
import org.apache.hadoop.security.TokenStorage;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

import skewtune.mapreduce.JobInProgress.JobType;
import skewtune.mapreduce.JobInProgress.ReactionContext;
import skewtune.mapreduce.JobInProgress.ReexecMap;
import skewtune.mapreduce.JobInProgress.ReexecReduce;
import skewtune.mapreduce.PartitionPlanner.ClusterInfo;
import skewtune.mapreduce.PartitionPlanner.Partition;
import skewtune.mapreduce.PartitionPlanner.Plan;
import skewtune.mapreduce.PartitionPlanner.PlanSpec;
import skewtune.mapreduce.lib.input.InputSplitCache;
import skewtune.mapreduce.protocol.HeartbeatResponse;
import skewtune.mapreduce.protocol.JobOnTaskTracker;
import skewtune.mapreduce.protocol.ReactiveMapOutput;
import skewtune.mapreduce.protocol.STTaskStatus;
import skewtune.mapreduce.protocol.SkewTuneClientProtocol;
import skewtune.mapreduce.protocol.SkewTuneTrackerProtocol;
import skewtune.mapreduce.protocol.TaskAction;
import skewtune.mapreduce.protocol.TaskStatusEvent;
import skewtune.mapreduce.protocol.TaskTrackerStatus;
import skewtune.mapreduce.server.jobtracker.JTConfig;
import skewtune.utils.Base64;

public class STJobTracker implements MRJobConfig, SkewTuneClientProtocol,
        SkewTuneTrackerProtocol, RefreshUserToGroupMappingsProtocol, JTConfig, TaskTrackerHttpResolver {

    static {
        org.apache.hadoop.mapreduce.util.ConfigUtil.loadResources();
        skewtune.mapreduce.util.ConfigUtil.loadResources();
    }

    private final long DELEGATION_TOKEN_GC_INTERVAL = 3600000; // 1 hour

    private final DelegationTokenSecretManager secretManager;

    // Approximate number of heartbeats that could arrive JobTracker
    // in a second
    private int NUM_HEARTBEATS_IN_SECOND;

    private final int DEFAULT_NUM_HEARTBEATS_IN_SECOND = 100;

    private final int MIN_NUM_HEARTBEATS_IN_SECOND = 1;

    // Scaling factor for heartbeats, used for testing only
    private float HEARTBEATS_SCALING_FACTOR;

    private final float MIN_HEARTBEATS_SCALING_FACTOR = 0.01f;

    private final float DEFAULT_HEARTBEATS_SCALING_FACTOR = 1.0f;

    public static enum State {
        INITIALIZING, RUNNING
    }

    State state = State.INITIALIZING;

    private static final int FS_ACCESS_RETRY_PERIOD = 10000;

    static final String JOB_INFO_FILE = "job-info";

    static final String LOCAL_SPLIT_FILE = "split.dta";

    static final String LOCAL_SPLIT_META_FILE = "split.info";

    static final String JOBFILE = "job.xml";

    static final String JOB_TOKEN_FILE = "jobToken";

    // system directory is completely owned by the JobTracker
    final static FsPermission SYSTEM_DIR_PERMISSION = FsPermission
            .createImmutable((short) 0700); // rwx------

    // system files should have 700 permission
    final static FsPermission SYSTEM_FILE_PERMISSION = FsPermission
            .createImmutable((short) 0700); // rwx------

    private MRAsyncDiskService asyncDiskService;

    public static final Log LOG = LogFactory.getLog(STJobTracker.class);

    /**
     * Start the JobTracker with given configuration.
     * 
     * The conf will be modified to reflect the actual ports on which the
     * JobTracker is up and running if the user passes the port as
     * <code>zero</code>.
     * 
     * @param conf
     *            configuration for the JobTracker.
     * @throws IOException
     */

    static STJobTracker startTracker(JobConf conf) throws IOException,
            InterruptedException {
        return startTracker(conf, generateNewIdentifier());
    }

    static STJobTracker startTracker(JobConf conf, String identifier)
            throws IOException, InterruptedException {
        STJobTracker result = null;
        while (true) {
            try {
                result = new STJobTracker(conf, identifier);
                break;
            } catch (VersionMismatch e) {
                throw e;
            } catch (BindException e) {
                throw e;
            } catch (UnknownHostException e) {
                throw e;
            } catch (AccessControlException ace) {
                // in case of jobtracker not having right access
                // bail out
                throw ace;
            } catch (IOException e) {
                LOG.warn("Error starting tracker: "
                        + StringUtils.stringifyException(e));
            }
            Thread.sleep(1000);
        }
        return result;
    }

    public void stopTracker() throws IOException {
        close();
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
            throws IOException {
        if (protocol.equals(SkewTuneTrackerProtocol.class.getName())) {
            return SkewTuneTrackerProtocol.versionID;
        } else if (protocol.equals(SkewTuneClientProtocol.class.getName())) {
            return SkewTuneClientProtocol.versionID;
        } else if (protocol.equals(RefreshAuthorizationPolicyProtocol.class
                .getName())) {
            return RefreshAuthorizationPolicyProtocol.versionID;
        } else if (protocol.equals(RefreshUserToGroupMappingsProtocol.class
                .getName())) {
            return RefreshUserToGroupMappingsProtocol.versionID;
        } else {
            throw new IOException("Unknown protocol to job tracker: "
                    + protocol);
        }
    }

    // ///////////////////////////////////////////////////////////////
    // The real JobTracker
    // //////////////////////////////////////////////////////////////
    int port;

    String localMachine;

    private final String trackerIdentifier;

    long startTime;

    int totalSubmissions = 0;

    //
    // Properties to maintain while running Jobs and Tasks:
    //
    // 1. Each Task is always contained in a single Job. A Job succeeds when all
    // its
    // Tasks are complete.
    //
    // 2. Every running or successful Task is assigned to a Tracker. Idle Tasks
    // are not.
    //
    // 3. When a Tracker fails, all of its assigned Tasks are marked as
    // failures.
    //
    // 4. A Task might need to be reexecuted if it (or the machine it's hosted
    // on) fails
    // before the Job is 100% complete. Sometimes an upstream Task can fail
    // without
    // reexecution if all downstream Tasks that require its output have already
    // obtained
    // the necessary files.
    //

    // (trackerID --> list of jobs to cleanup)
    Map<String, Set<JobID>> trackerToJobsToCleanup = new HashMap<String, Set<JobID>>();

    // (trackerID --> list of tasks to cleanup)
    Map<String, Set<TaskAttemptID>> trackerToTasksToCleanup = new HashMap<String, Set<TaskAttemptID>>();

    // jobs that are running
    Map<JobID, JobInProgress> jobs = new TreeMap<JobID, JobInProgress>();

    // (taskid --> trackerID)
    TreeMap<TaskAttemptID, String> taskidToTrackerMap = new TreeMap<TaskAttemptID, String>();
    
    // for caching task completion events
    HashSet<JobInProgress> originalJobs = new HashSet<JobInProgress>();
    
    HashSet<TaskID> pendingReactiveJob = new HashSet<TaskID>();
    HashMap<JobID,ScanTask> scanningJob = new HashMap<JobID,ScanTask>();
    
    // pending completed reactive map jobs
    Map<JobID,JobInProgress> pendingCompletedReactiveJob = new HashMap<JobID,JobInProgress>();
    
    // (trackerID --> last received heartbeat message
    TreeMap<String, TaskTrackerStatus> trackerToLastHeartbeat = new TreeMap<String, TaskTrackerStatus>();
    
    TreeMap<String, Integer> trackerToHttpPort = new TreeMap<String,Integer>();
    
    TreeMap<TaskAttemptID, TaskInProgress> taskidToTIP = new TreeMap<TaskAttemptID, TaskInProgress>();
    
    TreeMap<JobID,PlannedJob> plannedJobs = new TreeMap<JobID,PlannedJob>();
    
    // Watch and expire TaskTracker objects using these structures.
    // We can map from Name->TaskTrackerStatus, or we can expire by time.
    int totalMaps = 0;
    int totalReduces = 0;

    // Used to provide an HTML view on Job, Task, and TaskTracker structures
    final HttpServer infoServer;

    int infoPort;
    
    String defaultNotificationUrl;
    String defaultSpeculationEventUrl;
    String defaultSkewReportUrl;
    String trackerHttp;
    boolean speculativeSplit;

    Server interTrackerServer;

    // Some jobs are stored in a local system directory. We can delete
    // the files when we're done with the job.
    static final String SUBDIR = "skewtune-jt";

    FileSystem fs = null;

    Path systemDir = null;

    JobConf conf;

    private final UserGroupInformation mrOwner;

    private final String supergroup;

    // TODO currently SkewTune job tracker is associated with a single job
    // tracker. support multiple trackers?
    private final Cluster cluster;
    private final ClientProtocol jtClient;

    private boolean dumpHeartbeat;

    private static LocalDirAllocator lDirAlloc = new LocalDirAllocator(MRConfig.LOCAL_DIR);
    
    int reservedMaps;
    int reservedReduces;
    

    // TO BE USED BY TEST CLASSES ONLY
    // ONLY BUILD THE STATE WHICH IS REQUIRED BY TESTS
    STJobTracker() {
        infoServer = null;
        supergroup = null;
        trackerIdentifier = null;
        mrOwner = null;
        secretManager = null;
        cluster = null;
        jtClient = null;
    }

    /**
     * Start the JobTracker process, listen on the indicated port
     */
    STJobTracker(JobConf conf) throws IOException, InterruptedException {
        this(conf, generateNewIdentifier());
    }

    @SuppressWarnings("unchecked")
    STJobTracker(final JobConf conf, String jobtrackerIndentifier)
            throws IOException, InterruptedException {
        // find the owner of the process
        // get the desired principal to load
        String keytabFilename = conf.get(JTConfig.JT_KEYTAB_FILE);
        UserGroupInformation.setConfiguration(conf);
        if (keytabFilename != null) {
            String desiredUser = conf.get(JTConfig.JT_USER_NAME,
                    System.getProperty("user.name"));
            UserGroupInformation.loginUserFromKeytab(desiredUser,
                    keytabFilename);
            mrOwner = UserGroupInformation.getLoginUser();
        } else {
            mrOwner = UserGroupInformation.getCurrentUser();
        }

        supergroup = conf.get(MR_SUPERGROUP, "supergroup");
        LOG.info("Starting jobtracker with owner as "
                + mrOwner.getShortUserName() + " and supergroup as "
                + supergroup);

        long secretKeyInterval = conf.getLong(
                MRConfig.DELEGATION_KEY_UPDATE_INTERVAL_KEY,
                MRConfig.DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
        long tokenMaxLifetime = conf.getLong(
                MRConfig.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
                MRConfig.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
        long tokenRenewInterval = conf.getLong(
                MRConfig.DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
                MRConfig.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
        secretManager = new DelegationTokenSecretManager(secretKeyInterval,
                tokenMaxLifetime, tokenRenewInterval,
                DELEGATION_TOKEN_GC_INTERVAL);
        secretManager.startThreads();

        //
        // Grab some static constants
        //

        NUM_HEARTBEATS_IN_SECOND = conf.getInt(JT_HEARTBEATS_IN_SECOND,
                DEFAULT_NUM_HEARTBEATS_IN_SECOND);
        if (NUM_HEARTBEATS_IN_SECOND < MIN_NUM_HEARTBEATS_IN_SECOND) {
            NUM_HEARTBEATS_IN_SECOND = DEFAULT_NUM_HEARTBEATS_IN_SECOND;
        }

        HEARTBEATS_SCALING_FACTOR = conf.getFloat(JT_HEARTBEATS_SCALING_FACTOR,
                DEFAULT_HEARTBEATS_SCALING_FACTOR);
        if (HEARTBEATS_SCALING_FACTOR < MIN_HEARTBEATS_SCALING_FACTOR) {
            HEARTBEATS_SCALING_FACTOR = DEFAULT_HEARTBEATS_SCALING_FACTOR;
        }

        // whether to dump or not every heartbeat message even when DEBUG is enabled
        dumpHeartbeat = conf.getBoolean(JT_HEARTBEATS_DUMP, false);

        // This is a directory of temporary submission files. We delete it
        // on startup, and can delete any files that we're done with
        this.conf = conf;
        JobConf jobConf = new JobConf(conf);

        // Set ports, start RPC servers, setup security policy etc.
        InetSocketAddress addr = getAddress(conf);
        this.localMachine = addr.getHostName();
        this.port = addr.getPort();

        int handlerCount = conf.getInt(JT_IPC_HANDLER_COUNT, 10);
        this.interTrackerServer = RPC.getServer(SkewTuneClientProtocol.class,
                this, addr.getHostName(), addr.getPort(), handlerCount, false,
                conf, secretManager);
        if (LOG.isDebugEnabled()) {
            Properties p = System.getProperties();
            for (Iterator it = p.keySet().iterator(); it.hasNext();) {
                String key = (String) it.next();
                String val = p.getProperty(key);
                LOG.debug("Property '" + key + "' is " + val);
            }
        }

        InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(conf.get(
                JT_HTTP_ADDRESS, String.format("%s:0",this.localMachine)));
        String infoBindAddress = infoSocAddr.getHostName();
        int tmpInfoPort = infoSocAddr.getPort();
        this.startTime = System.currentTimeMillis();
        infoServer = new HttpServer("job", infoBindAddress, tmpInfoPort,
                tmpInfoPort == 0, conf);
        infoServer.setAttribute("job.tracker", this);
        infoServer.addServlet("jobcompletion", "/completion", JobCompletionServlet.class);
        infoServer.addServlet("taskspeculation", "/speculation", SpeculationEventServlet.class);
        infoServer.addServlet("skewreport", "/skew", SkewReportServlet.class);
        infoServer.addServlet("tasksplit", "/split/*", SplitTaskServlet.class);
        infoServer.addServlet("tasksplitV2", "/splitV2/*", SplitTaskV2Servlet.class);
        infoServer.start();

        this.trackerIdentifier = jobtrackerIndentifier;

        // The rpc/web-server ports can be ephemeral ports...
        // ... ensure we have the correct info
        this.port = interTrackerServer.getListenerAddress().getPort();
        this.conf.set(JT_IPC_ADDRESS, (this.localMachine + ":" + this.port));
        LOG.info("JobTracker up at: " + this.port);
        this.infoPort = this.infoServer.getPort();
        this.conf.set(JT_HTTP_ADDRESS, infoBindAddress + ":" + this.infoPort);
        LOG.info("JobTracker webserver: " + this.infoServer.getPort());
        this.defaultNotificationUrl = String.format("http://%s:%d/completion?jobid=$jobId&status=$jobStatus",infoBindAddress,this.infoPort);
        LOG.info("JobTracker completion URI: "+defaultNotificationUrl);
//        this.defaultSpeculationEventUrl = String.format("http://%s:%d/speculation?taskid=$taskId&remainTime=$taskRemainTime",infoBindAddress,this.infoPort);
        this.defaultSpeculationEventUrl = String.format("http://%s:%d/speculation?jobid=$jobId",infoBindAddress,this.infoPort);
        LOG.info("JobTracker speculation event URI: "+defaultSpeculationEventUrl);
        this.defaultSkewReportUrl = String.format("http://%s:%d/skew",infoBindAddress,this.infoPort);
        LOG.info("JobTracker skew report event URI: "+defaultSkewReportUrl);
        this.trackerHttp = String.format("http://%s:%d", infoBindAddress, this.infoPort);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // if we haven't contacted the namenode go ahead and do it
                if (fs == null) {
                    fs = mrOwner
                            .doAs(new PrivilegedExceptionAction<FileSystem>() {
                                @Override
                                public FileSystem run() throws IOException {
                                    return FileSystem.get(conf);
                                }
                            });
                }

                // clean up the system dir, which will only work if hdfs is out
                // of safe mode
                if (systemDir == null) {
                    systemDir = new Path(getSystemDir());
                }
                try {
                    FileStatus systemDirStatus = fs.getFileStatus(systemDir);
                    if (!systemDirStatus.getOwner().equals(
                            mrOwner.getShortUserName())) {
                        throw new AccessControlException("The systemdir "
                                + systemDir + " is not owned by "
                                + mrOwner.getShortUserName());
                    }
                    if (!systemDirStatus.getPermission().equals(
                            SYSTEM_DIR_PERMISSION)) {
                        LOG.warn("Incorrect permissions on " + systemDir
                                + ". Setting it to " + SYSTEM_DIR_PERMISSION);
                        fs.setPermission(systemDir, new FsPermission(
                                SYSTEM_DIR_PERMISSION));
                    } else {
                        break;
                    }
                } catch (FileNotFoundException fnf) {
                } // ignore
            } catch (AccessControlException ace) {
                LOG.warn("Failed to operate on " + JTConfig.JT_SYSTEM_DIR + "("
                        + systemDir + ") because of permissions.");
                LOG.warn("Manually delete the " + JTConfig.JT_SYSTEM_DIR + "("
                        + systemDir + ") and then start the JobTracker.");
                LOG.warn("Bailing out ... ");
                throw ace;
            } catch (IOException ie) {
                LOG.info("problem cleaning system directory: " + systemDir, ie);
            }
            Thread.sleep(FS_ACCESS_RETRY_PERIOD);
        }

        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }

        // initialize cluster variable
        cluster = new Cluster(this.conf);
        
        // now create a job client proxy
        jtClient = (ClientProtocol) RPC.getProxy(ClientProtocol.class,
                ClientProtocol.versionID, JobTracker.getAddress(conf), mrOwner,
                this.conf, NetUtils.getSocketFactory(conf, ClientProtocol.class));
        
        new SpeculativeScheduler().start();
        
        // initialize task event fetcher
        new TaskCompletionEventFetcher().start();
        

        // Same with 'localDir' except it's always on the local disk.
        asyncDiskService = new MRAsyncDiskService(FileSystem.getLocal(conf),
                conf.getLocalDirs());
        asyncDiskService.moveAndDeleteFromEachVolume(SUBDIR);

        // keep at least one asynchronous worker per CPU core
        int numProcs = Runtime.getRuntime().availableProcessors();
        LOG.info("# of available processors = "+numProcs);
        int maxFactor = conf.getInt(JT_MAX_ASYNC_WORKER_FACTOR, 2);
        asyncWorkers = new ThreadPoolExecutor(numProcs, numProcs * maxFactor, 30, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(true),
                new ThreadPoolExecutor.CallerRunsPolicy());
        
        speculativeSplit = conf.getBoolean(JT_SPECULATIVE_SPLIT, false);
    }

    private static SimpleDateFormat getDateFormat() {
        return new SimpleDateFormat("yyyyMMddHHmm");
    }

    private static String generateNewIdentifier() {
        return getDateFormat().format(new Date());
    }

    static boolean validateIdentifier(String id) {
        try {
            // the jobtracker id should be 'date' parseable
            getDateFormat().parse(id);
            return true;
        } catch (ParseException pe) {
        }
        return false;
    }

    static boolean validateJobNumber(String id) {
        try {
            // the job number should be integer parseable
            Integer.parseInt(id);
            return true;
        } catch (IllegalArgumentException pe) {
        }
        return false;
    }

    /**
     * Get JobTracker's FileSystem. This is the filesystem for
     * mapreduce.system.dir.
     */
    FileSystem getFileSystem() {
        return fs;
    }

    /**
     * Get JobTracker's LocalFileSystem handle. This is used by jobs for
     * localizing job files to the local disk.
     */
    LocalFileSystem getLocalFileSystem() throws IOException {
        return FileSystem.getLocal(conf);
    }

    public static InetSocketAddress getAddress(Configuration conf) {
        String jobTrackerStr = conf.get(JT_IPC_ADDRESS, "localhost:9012");
        return NetUtils.createSocketAddr(jobTrackerStr);
    }

    /**
     * Run forever
     */
    public void offerService() throws InterruptedException, IOException {
        // Prepare for recovery. This is done irrespective of the status of
        // restart
        // flag.

        // start the inter-tracker server once the jt is ready
        this.interTrackerServer.start();

        synchronized (this) {
            state = State.RUNNING;
        }
        LOG.info("Starting RUNNING");

        this.interTrackerServer.join();
        LOG.info("Stopped interTrackerServer");
    }

    void close() throws IOException {
        if (this.infoServer != null) {
            LOG.info("Stopping infoServer");
            try {
                this.infoServer.stop();
            } catch (Exception ex) {
                LOG.warn("Exception shutting down JobTracker", ex);
            }
        }
        if (this.interTrackerServer != null) {
            LOG.info("Stopping interTrackerServer");
            this.interTrackerServer.stop();
        }

        LOG.info("stopped all jobtracker services");
        return;
    }

    // /////////////////////////////////////////////////////
    // Accessors for objects that want info on jobs, tasks,
    // trackers, etc.
    // /////////////////////////////////////////////////////
    public int getTotalSubmissions() {
        return totalSubmissions;
    }

    public String getJobTrackerMachine() {
        return localMachine;
    }

    /**
     * Get the unique identifier (ie. timestamp) of this job tracker start.
     * 
     * @return a string with a unique identifier
     */
    public String getTrackerIdentifier() {
        return trackerIdentifier;
    }

    public int getTrackerPort() {
        return port;
    }

    public int getInfoPort() {
        return infoPort;
    }

    public long getStartTime() {
        return startTime;
    }
    
    public Cluster getCluster() {
        return cluster;
    }

    // //////////////////////////////////////////////////
    // InterTrackerProtocol
    // //////////////////////////////////////////////////

    public String getBuildVersion() throws IOException {
        return VersionInfo.getBuildVersion();
    }

    /**
     * Grab the local fs name
     */
    public synchronized String getFilesystemName() throws IOException {
        if (fs == null) {
            throw new IllegalStateException(
                    "FileSystem object not available yet");
        }
        return fs.getUri().toString();
    }

    /**
     * Remove the job_ from jobids to get the unique string.
     */
    static String getJobUniqueString(String jobid) {
        return jobid.substring(4);
    }

    /**
     * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getSystemDir()
     */
    public String getSystemDir() {
        Path sysDir = new Path(conf.get(JTConfig.JT_SYSTEM_DIR,
                "/tmp/hadoop/mapred/system"));
        return fs.makeQualified(sysDir).toString();
    }

    /**
     * @throws LoginException
     * @see org.apache.hadoop.mapreduce.protocol.ClientProtocol#getStagingAreaDir()
     */
    public String getStagingAreaDir() throws IOException {
        try {
            final String user = UserGroupInformation.getCurrentUser()
                    .getShortUserName();
            return mrOwner.doAs(new PrivilegedExceptionAction<String>() {
                @Override
                public String run() throws Exception {
                    Path stagingRootDir = new Path(conf.get(
                            JTConfig.JT_STAGING_AREA_ROOT,
                            "/tmp/hadoop/mapred/staging"));
                    FileSystem fs = stagingRootDir.getFileSystem(conf);
                    return fs.makeQualified(
                            new Path(stagingRootDir, user + "/.staging"))
                            .toString();
                }
            });
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
    }

    // /////////////////////////////////////////////////////////////
    // JobTracker methods
    // /////////////////////////////////////////////////////////////

    // Get the job directory in system directory
    Path getSystemDirectoryForJob(org.apache.hadoop.mapreduce.JobID id) {
        return new Path(getSystemDir(), id.toString());
    }

    // Get the job token file in system directory
    Path getSystemFileForJob(org.apache.hadoop.mapreduce.JobID id) {
        return new Path(getSystemDirectoryForJob(id) + "/" + JOB_INFO_FILE);
    }

    /**
     * Is the calling user a super user? Or part of the supergroup?
     * 
     * @return true, if it is a super user
     */
    static boolean isSuperUserOrSuperGroup(UserGroupInformation callerUGI,
            UserGroupInformation superUser, String superGroup) {
        if (superUser.getShortUserName().equals(callerUGI.getShortUserName())) {
            return true;
        }
        String[] groups = callerUGI.getGroupNames();
        for (int i = 0; i < groups.length; ++i) {
            if (groups[i].equals(superGroup)) {
                return true;
            }
        }
        return false;
    }

    UserGroupInformation getMROwner() {
        return mrOwner;
    }

    String getSuperGroup() {
        return supergroup;
    }

    public static String getUserDir(String user) {
        return SUBDIR + Path.SEPARATOR + user;
    }

    public static String getLocalJobDir(String user, String jobid) {
        return getUserDir(user) + Path.SEPARATOR + jobid;
    }

    public static String getLocalJobTokenFile(String user, String jobid) {
        return getLocalJobDir(user, jobid) + Path.SEPARATOR + JOB_TOKEN_FILE;
    }

    public static String getLocalSplitFile(String user, String jobid) {
        return getLocalJobDir(user, jobid) + Path.SEPARATOR + LOCAL_SPLIT_FILE;
    }

    public static String getLocalSplitMetaFile(String user, String jobid) {
        return getLocalJobDir(user, jobid) + Path.SEPARATOR
                + LOCAL_SPLIT_META_FILE;
    }

    // //////////////////////////////////////////////////////////
    // main()
    // //////////////////////////////////////////////////////////

    /**
     * Start the JobTracker process. This is used only for debugging. As a rule,
     * JobTracker should be run as part of the DFS Namenode process.
     */
    public static void main(String argv[]) throws IOException,
            InterruptedException {
        StringUtils.startupShutdownMessage(STJobTracker.class, argv, LOG);

        try {
            if (argv.length == 0) {
                STJobTracker tracker = startTracker(new JobConf());
                tracker.offerService();
            } else {
                if ("-dumpConfiguration".equals(argv[0]) && argv.length == 1) {
                    dumpConfiguration(new PrintWriter(System.out));
                    System.out.println();
                } else {
                    System.out
                            .println("usage: JobTracker [-dumpConfiguration]");
                    System.exit(-1);
                }
            }
        } catch (Throwable e) {
            LOG.fatal(StringUtils.stringifyException(e));
            System.exit(-1);
        }
    }

    /**
     * Dumps the configuration properties in Json format
     * 
     * @param writer
     *            {@link}Writer object to which the output is written
     * @throws IOException
     */
    private static void dumpConfiguration(Writer writer) throws IOException {
        Configuration.dumpConfiguration(new JobConf(), writer);
        writer.write("\n");
    }

    @Override
    public void refreshUserToGroupsMappings(Configuration conf)
            throws IOException {
        LOG.info("Refreshing all user-to-groups mappings. Requested by user: "
                + UserGroupInformation.getCurrentUser().getShortUserName());

        Groups.getUserToGroupsMappingService(conf).refresh();
    }

    /**
     * Discard a current delegation token.
     */
    @Override
    public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
            throws IOException, InterruptedException {
        String user = UserGroupInformation.getCurrentUser().getUserName();
        secretManager.cancelToken(token, user);
    }

    /**
     * Get a new delegation token.
     */
    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
            throws IOException, InterruptedException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        Text owner = new Text(ugi.getUserName());
        Text realUser = null;
        if (ugi.getRealUser() != null) {
            realUser = new Text(ugi.getRealUser().getUserName());
        }
        DelegationTokenIdentifier ident = new DelegationTokenIdentifier(owner,
                renewer, realUser);
        return new Token<DelegationTokenIdentifier>(ident, secretManager);
    }

    /**
     * Renew a delegation token to extend its lifetime.
     */
    @Override
    public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
            throws IOException, InterruptedException {
        String user = UserGroupInformation.getCurrentUser().getUserName();
        return secretManager.renewToken(token, user);
    }

    /*
     * skew reduce logic
     */
    
    /**
     * should hold the lock on tracker object by heartbeat()
     * @param tip
     * @param trackerName
     */
    void createTaskEntry(TaskAttemptID taskid,String taskTracker,TaskInProgress tip) {
        this.taskidToTIP.put(taskid, tip);
        JobID jobid = taskid.getJobID();
        synchronized ( plannedJobs ) {
            PlannedJob job = this.plannedJobs.get(jobid);
            if ( job != null && job.remove(taskid) ) {
                this.plannedJobs.remove(jobid);
            }
        }
    }
    
    void removeTaskEntry(TaskAttemptID taskid) {
        taskidToTIP.remove(taskid);
    }
    
    PartitionPlanner.ClusterInfo getClusterAvailability(ReactionContext context,long now) throws IOException, InterruptedException {
        ClusterMetrics metrics = cluster.getClusterStatus();
        TaskAttemptID attemptId = context.getTargetAttemptID();
        TaskType type = attemptId == null ? context.getTaskID().getTaskType() : attemptId.getTaskType();
        
        int maxSlots = type == TaskType.MAP ? metrics.getMapSlotCapacity() : metrics.getReduceSlotCapacity();
        int runningSlots = type == TaskType.MAP ? metrics.getRunningMaps() : metrics.getRunningReduces();
        int runningSkewTune = 0;
        double[] remainingTimes = new double[maxSlots];
        int from = maxSlots;
        
        // if this is a speculative REDUCE, the original slot becomes available. We should make it available.
        boolean availRightNow = attemptId != null && type == TaskType.REDUCE && context.getTimePerByte() == 0.f;
        
        synchronized (this) {
            // FIXME this only involves tasks that are scheduled and running
            // we should keep an expected information as well.
            
            // on planning, we should add the planned tasks and getClusterAvailability should
            // incorporate any planned stuffs in it.
            
            // the information required:
            // Map<JobID, [long planned at, for tasks -- estimated runtime]>
            // on first heartbeat from each task, we remove each information.
            
            for ( Map.Entry<TaskAttemptID, TaskInProgress> e : taskidToTIP.entrySet() ) {
                TaskAttemptID taskid = e.getKey();
                if ( taskid.getTaskType() == type ) {
                    // extra check
                    if ( availRightNow && taskid.equals(attemptId) ) continue; // this will become available immediately
                    
                    TaskInProgress tip = e.getValue();
                    double t = tip.getRemainingTime(taskid, now);
                    if ( t > 0. ) {
                        remainingTimes[--from] = tip.getRemainingTime(taskid, now);
                        ++runningSkewTune;
                        if ( from == 0 ) break;
                    }
                }
            }
            if ( from > 0 ) {
                synchronized (plannedJobs) {
                    for ( Map.Entry<JobID, PlannedJob> e : this.plannedJobs.entrySet() ) {
                        PlannedJob plan = e.getValue();
                        from = plan.fillCompletionTime(type, now, remainingTimes, from);
                        if ( from == 0 ) break;
                    }
                }
            }
        }
        Arrays.sort(remainingTimes, from, maxSlots);
        
        if ( LOG.isDebugEnabled() ) {
            LOG.debug("cluster availability = "+Arrays.toString(remainingTimes));
        }
        
        // FIXME incorporate other tasks that are not SkewTune
        
        return new PartitionPlanner.ClusterInfo(type, maxSlots, runningSlots, runningSkewTune, remainingTimes, maxSlots);
    }

    /**
     * SkewTune heartbeat protocol
     * 
     * REQUEST (Heartbeat)
     * 
     * HOST TaskAttemptID -- status report (initialization|mapoutput|completed)
     * progress [splitted] TaskAttemptID (initialization|mapoutput|completed)
     * progress [splitted] ...
     * 
     * RESPONSE
     * 
     * TaskAttemptID (keep going | new map output [] | cancel )
     * 
     * .skewtune/m-0000?/part-m-XXXXX ...
     * 
     * The protocol is softstate. Jobtracker responds to each heartbeat with the
     * task to cancel and list of unknown jobs in the heart beat message. The
     * task tracker is supposed to reclaim space occupied by the unknown jobs.
     */

    @Override
    public synchronized HeartbeatResponse heartbeat(TaskTrackerStatus status,
            boolean justStarted, boolean justInited, short responseId)
            throws IOException, InterruptedException {
        if (LOG.isDebugEnabled() && dumpHeartbeat ) {
            LOG.debug("Got heartbeat from: " + status.getTrackerName()
                    + " with responseId: " + responseId);
        }

        String trackerName = status.getTrackerName();
        long now = System.currentTimeMillis();

        short newResponseId = (short) (responseId + 1);
        status.setLastSeen(now);

        trackerToLastHeartbeat.put(trackerName, status);
        trackerToHttpPort.put(trackerName, status.getHttpPort());

        HashSet<JobID> unknownJobs = new HashSet<JobID>();
        ArrayList<ReactiveMapOutput> newMapOutput = new ArrayList<ReactiveMapOutput>();
//        ArrayList<TaskAttemptID> cancelledTasks = new ArrayList<TaskAttemptID>();
        ArrayList<TaskAction> taskActions = new ArrayList<TaskAction>();
        ArrayList<TaskStatusEvent> newTakeOver = new ArrayList<TaskStatusEvent>();

        // per job -- processing

        // FIXME retrieve task tracker
        // FIXME for each job, update task status, build host-task map
        for (JobOnTaskTracker jobReport : status.getJobReports()) {
            JobID jobid = jobReport.getJobID();
            JobInProgress jip = null;
            boolean pendingReactive = false;
            synchronized (jobs) {
                jip = jobs.get(jobid);
            }
            
            if ( jip == null ) {
                synchronized (pendingCompletedReactiveJob) {
                    jip = pendingCompletedReactiveJob.get(jobid);
                }
                pendingReactive = jip != null;
            }
            
            if (jip == null) {
                // FIXME check the pending completion list
                unknownJobs.add(jobid); // this job must be cleared
            } else {
                int from = jobReport.getFromIndex();
                int fromTakeOver = jobReport.getFromIndexOfTakeOver();
                final JobType jobType = jip.getJobType();
                BitSet completed = new BitSet(jip.getNumMapTasks());
                
                synchronized (jip) {
                    // load job token into this node
                    if ( jobType == JobType.ORIGINAL || jobType == JobType.REDUCE_REACTIVE ) {
                        scheduleJobTokenLoading(jip); // we only need to load it for original job
                        // FIXME we need to load it for other job if we support recursive split
                    }
                    
                    // update statistics of this task
                    for (STTaskStatus taskStatus : jobReport.getTaskReports()) {
                        int action = jip.handleTaskHeartbeat(taskStatus,status.getHostName(),completed);
                        if ( action != 0 ) {
                            taskActions.add(new TaskAction(taskStatus.getTaskID(),action));
                        }
//                        if ( jip.handleTaskHeartbeat(taskStatus,status.getHostName(),completed) != 0) {
//                            cancelledTasks.add(taskStatus.getTaskID());
                            // FIXME create task action
//                        }
                    }
                    // fetch all available new map output from FROM
                    if (from >= 0) {
                        jip.retrieveNewMapOutput(newMapOutput, from);
                    }
                    if (fromTakeOver >= 0) {
                        jip.retrieveNewTakeOver(newTakeOver, fromTakeOver);
                    }
                    
                    if ( jobType == JobType.MAP_REACTIVE && pendingReactive ) {
                        if ( jip.isAllMapOutputIndexAvailable() ) {
                            synchronized (pendingCompletedReactiveJob) {
                                pendingCompletedReactiveJob.remove(jobid);
                            }
                            cleanupPendingReactiveMap(jip);
                        }
                    }
                }
                
//                if ( jobType == JobType.ORIGINAL ) {
//                    jip.notifyMapCompletion(completed);
//                }
            }
        }

        int nextInterval = getNextHeartbeatInterval();

        return new HeartbeatResponse(
                newResponseId,
                nextInterval,
                newMapOutput.toArray(new ReactiveMapOutput[newMapOutput.size()]),
//                cancelledTasks.toArray(new TaskAttemptID[cancelledTasks.size()]),
                taskActions.toArray(new TaskAction[taskActions.size()]),
                unknownJobs.toArray(new JobID[unknownJobs.size()]),
                newTakeOver.toArray(new TaskStatusEvent[newTakeOver.size()]));
    }

    /**
     * Calculates next heartbeat interval using cluster size. Heartbeat interval
     * is incremented by 1 second for every 100 nodes by default.
     * 
     * @return next heartbeat interval.
     */
    public int getNextHeartbeatInterval() {
        int clusterSize = clusterMetrics == null ? 0 : clusterMetrics
                .getTaskTrackerCount();
        // get the no of task trackers
        int heartbeatInterval = Math
                .max((int) (1000 * HEARTBEATS_SCALING_FACTOR * Math
                        .ceil((double) clusterSize / NUM_HEARTBEATS_IN_SECOND)),
                        3000);
        return heartbeatInterval;
    }

    @Override
    public void submitJob(JobID jobId, Configuration conf) throws IOException,
            InterruptedException {
        // initialize task
        // setup notification URL
        conf.set(END_NOTIFICATION_URL, this.defaultNotificationUrl);
        Job runningJob = cluster.getJob(jobId);
        JobInProgress jip = new JobInProgress(this, runningJob, conf);
        jip.eventSplitMeta = this.asyncWorkers.submit(new LoadInputSplitMeta(jip));

        // now append to the job
        synchronized (jobs) {
            jobs.put(jobId, jip);
        }
        
        synchronized (originalJobs) {
            originalJobs.add(jip);
        }
        
        // FIXME reserve maps and reduces accordingly

        LOG.info("job has submitted "+jobId);
        // localizeJobFiles(jip,true); // should we always cache the split?
    }

    @Override
    public void killJob(JobID jobid) throws IOException, InterruptedException {
        JobInProgress jip = null;
        synchronized (jobs) {
            jip = jobs.get(jobid);
        }
        if (jip == null) {
            LOG.warn("Unknown jobid: " + jobid.toString());
        } else {
            jip.kill(true, pendingCompletedReactiveJob);
        }
    }

    private Future<JobID> fastSplitTask(TaskID taskid,int n) throws IOException, InterruptedException {
        JobInProgress jip = null;
        synchronized (jobs) {
            jip = jobs.get(taskid.getJobID());
        }
        
        if ( jip == null ) {
            String msg = "unknown task " + taskid;
            LOG.error(msg);
            throw new IOException(msg);
        }
        
        TaskInProgress tip = jip.getTaskInProgress(taskid);
        ReactionContext context = taskid.getTaskType() == TaskType.MAP ? new ReexecMap(tip,n) : new ReexecReduce(tip);
        return fastSplitTask(context,true);

//        return fastSplitTask(taskid,n,true);
    }
/*
    private Future<JobID> fastSplitTask(TaskID taskid, int n, boolean speculative) throws IOException,
            InterruptedException {
        JobInProgress jip = null;
        synchronized (jobs) {
            jip = jobs.get(taskid.getJobID());
        }
        
        if ( jip == null ) {
            String msg = "unknown task " + taskid;
            LOG.error(msg);
            throw new IOException(msg);
        }
        
        synchronized (pendingReactiveJob) {
            if ( ! jip.canSpeculateThis(taskid)
                    || pendingReactiveJob.contains(taskid)
                    || jip.hasReactiveJob(taskid) ) { // being paranoid.
                LOG.warn("reactive job is already scheduled or running for "+taskid);
                return null;
            }
            pendingReactiveJob.add(taskid);
        }
        
        if ( LOG.isInfoEnabled() ) {
            LOG.info(String.format("split task %s into %d tasks",taskid.toString(),n));
        }
        
        // FIXME split the task using asynchronous task
        // check whether both job token and meta data has been loaded
        
        JobID jobid = null;
        try {
            jip.waitUntilReadyToSplit(taskid);
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("scheduling asynchronous split task for task "+taskid);
            }
            TaskInProgress tip = jip.getTaskInProgress(taskid);
            ReactionContext context = taskid.getTaskType() == TaskType.MAP ? new ReexecMap(tip,n) : new ReexecReduce(tip);
            return this.asyncWorkers.submit(new SplitTask(context, speculative));
//            return this.asyncWorkers.submit(new SplitTask(jip, taskid, n, context, speculative));
        } catch (ExecutionException e) {
            throw new IOException(e.getCause()); // wrap again!
        }
    }
*/
    
    private Future<JobID> fastSplitTask(ReactionContext context,boolean speculative) throws IOException, InterruptedException {
        JobInProgress jip = context.getJob();
        TaskID taskid = context.getTaskID();
        
        synchronized (pendingReactiveJob) {
            if ( ! jip.canSpeculateThis(taskid)
                    || pendingReactiveJob.contains(taskid)
                    || jip.hasReactiveJob(taskid) ) { // being paranoid.
                LOG.warn("reactive job is already scheduled or running for "+taskid);
                return null;
            }
            pendingReactiveJob.add(taskid);
        }
        
        
        // FIXME split the task using asynchronous task
        // check whether both job token and meta data has been loaded
        
        JobID jobid = null;
        try {
            jip.waitUntilReadyToSplit(taskid);
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("scheduling asynchronous split task for task "+taskid);
            }
//            
//            long now = System.currentTimeMillis();
//            ClusterInfo clusterInfo = this.getClusterInfo(context,now);
//            Plan p = PartitionPlanner.plan(context, clusterInfo, now);
//            
//            if ( LOG.isInfoEnabled() ) {
//                LOG.info(String.format("split task %s into %d tasks",taskid.toString(),p.getNumPartitions()));
//            }

            return this.asyncWorkers.submit(new SplitTask( context, speculative));

//            return this.asyncWorkers.submit(new SplitTask(jip, taskid, p.getNumPartitions(), context, speculative));
        } catch (ExecutionException e) {
            throw new IOException(e.getCause()); // wrap again!
        }
    }

    
    
    private Future<JobID> launchScanTask(JobInProgress jip,TaskID taskid,JobInProgress.ReactionContext action) throws IOException, InterruptedException {
        return this.asyncWorkers.submit(new ScanTask(jip, taskid, action));
    }

    private Future<JobID> launchPlanAndLaunchTask(JobInProgress jip,TaskID taskid,JobInProgress.ReactionContext action) throws IOException, InterruptedException {
        return this.asyncWorkers.submit(new PlanAndLaunchTask(jip, taskid, action));
    }
    
    private Future<JobID> launchPlanAndLaunchTask(ScanTask scanTask) throws IOException, InterruptedException {
        return this.asyncWorkers.submit(new PlanAndLaunchTask(scanTask));
    }
    
    @Override
    public JobID splitTask(TaskID taskid, int n) throws IOException,
            InterruptedException {
        try {
            JobID jobid = fastSplitTask(taskid,n).get();
            if ( jobid != null ) {
                LOG.info("new splitted job "+jobid);
            }
            return jobid;
        } catch ( ExecutionException e) {
            throw new IOException(e.getCause()); // wrap again!
        }
    }

    /*
    private Future<JobID> scheduleSpeculativeTask(JobID jobid) throws IOException, InterruptedException {
        JobInProgress jip = null;
        synchronized (jobs) {
            jip = jobs.get(jobid);
        }

        if ( jip == null ) {
            String msg = "unknown job "+jobid;
            LOG.error(msg);
            throw new IOException(msg);
        }
        
        STTaskStatus taskStatus = jip.findSpeculativeTask();
        if ( taskStatus == null ) {
            LOG.debug("Nothing to speculate for "+jobid);
            return null;
        }
        
        TaskID taskid = taskStatus.getTaskID().getTaskID();
        
        synchronized (pendingReactiveJob) {
            if ( pendingReactiveJob.contains(taskid) || jip.hasReactiveJob(taskid) ) {
                LOG.warn("reactive job is already running for "+taskid);
                return null;
            }
            pendingReactiveJob.add(taskid);
        }
        
        int n;
        if ( taskid.getTaskType() == TaskType.MAP ) {
            n = clusterMetrics.getMapSlotCapacity() - 1; // exclude original task
        } else {
            n = clusterMetrics.getReduceSlotCapacity() - 1; // exclude original task
        }
        
        if ( LOG.isInfoEnabled() ) {
            LOG.info(String.format("split task %s into %d tasks",taskid.toString(),n));
        }
        
        // FIXME split the task using asynchronous task
        // check whether both job token and meta data has been loaded
        
        try {
            jip.waitUntilReadyToSplit(taskid);
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("scheduling asynchronous split task for task "+taskid);
            }
            return this.asyncWorkers.submit(new SplitTask(jip, taskid, n));
        } catch (ExecutionException e) {
            throw new IOException(e.getCause()); // wrap again!
        }
}*/

    ThreadPoolExecutor asyncWorkers; // mirroring job token?
    
    private void cleanupPendingReactiveMap(JobInProgress jip) {
        if ( jip.getJobType() != JobType.MAP_REACTIVE ) {
            throw new IllegalStateException("Invalid job type "+jip.getJobType());
        }
        if ( ! jip.isAllMapOutputIndexAvailable() ) {
            throw new IllegalStateException("Reactive map "+jip.getJobID()+" is cleaned up before receiving all map output indexes");
        }
        
        if ( LOG.isInfoEnabled() ) {
            LOG.info("Pending reactive map "+jip.getJobID()+" received all map output indexes");
        }
        
        jip.notifyParentJob(false);
        
        // we don't need a map at the end of parent job
        if ( jip.parent.getNumReduceTasks() > 0 ) {
            jip.parent.addPathToDelete( jip.getOutputPath() );
        }
    
        jip.cleanup();
    }

    public static class JobCompletionServlet extends HttpServlet {
        private static final long serialVersionUID = -7533419814261205808L;

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            String jobIdStr = req.getParameter("jobid");
            if (jobIdStr == null)
                return;
            final JobID jobId = JobID.forName(jobIdStr);
            String jobStatus = req.getParameter("status");

            if (LOG.isInfoEnabled()) {
                LOG.info("jobid = " + jobId + "; jobStatus = " + jobStatus);
            }

            final STJobTracker tracker = (STJobTracker) getServletContext().getAttribute("job.tracker");

            // look up this job
            JobInProgress thisJob = null;
            synchronized (tracker.jobs) {
                thisJob = tracker.jobs.remove(jobId); // this has been
                                                      // completed. so we can
                                                      // safely remove
            }
            synchronized (tracker.plannedJobs) {
                tracker.plannedJobs.remove(jobId);
            }
            
            boolean cleanup = true;

            if (thisJob == null) {
                
                ScanTask scanTask = null;
                synchronized ( tracker.scanningJob ) {
                    scanTask = tracker.scanningJob.remove(jobId);
                }

                if ( scanTask == null ) {
                    if ( LOG.isDebugEnabled()) {
                        LOG.debug("completion of unknown job " + jobId);
                    }
                    
                    resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
                } else {
                    // scan is done. schedule planning and repartition task
                    if ( "SUCCEEDED".equals(jobStatus) ) {
                        // schedule planning
                        tracker.schedulePlanAndLaunch(scanTask); 
                    } else {
                        // scan failed
                        LOG.warn("scan failed for task "+scanTask.getTaskID());
                        try {
                            scanTask.getJobInProgress().getOriginalJob().kill();
                        } catch (InterruptedException ignore) {}
                    }
                }
            } else {
                JobInProgress.JobType jobType = thisJob.getJobType();
                
                // TODO schedule suicide!!!
                if (jobType == JobInProgress.JobType.ORIGINAL) {
                    // FIXME propagate notification URL
                    // FIXME should remove temporary files on exit

                    // if dependent jobs are running, then should wait for all
                    // dependent jobs killed. then remove the final output
                    // directory.
                    
                    synchronized (tracker.originalJobs) {
                        tracker.originalJobs.remove(thisJob); // just in case
                    }

                    if ( LOG.isDebugEnabled() ) {
                        LOG.debug("Original job "+jobId+" has been "+jobStatus);
                    }
                    
                    try {
                        thisJob.kill(true, tracker.pendingCompletedReactiveJob ); // if there is any remaining reactive jobs
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    tracker.pendingTakeOvers.unregister(jobId);

//                    if ( ! "SUCCEEDED".equals(jobStatus) ) {
                    // purge all temporary output
                    thisJob.deleteAll(tracker.getFileSystem());
//                    }
                } else {
                    int partition = thisJob.getPartition();
                    boolean cleanOutputFile = false;

                    if (jobType == JobInProgress.JobType.MAP_REACTIVE) {
                        if ( thisJob.isAllMapOutputIndexAvailable() ) {
                            if ( LOG.isDebugEnabled() ) {
                                LOG.debug("Reactive map "+jobId+" has been "+jobStatus);
                            }
                            
                            if ("SUCCEEDED".equals(jobStatus)) {
                                thisJob.notifyParentJob(false);
                            }
                            
                            // we don't need a map at the end of parent job
                            if ( thisJob.parent.getNumReduceTasks() > 0 ) {
                                cleanOutputFile = true;
                            }
                        } else {
                            if ( "SUCCEEDED".equals(jobStatus) ) {
                                if ( LOG.isInfoEnabled() ) {
                                    LOG.info("Reactive map "+jobId+" has been "+jobStatus+" but not all map output index have been retrieved");
                                }
                                
                                // append this job to pending completion. should wait until we retrieve map output indexes
                                synchronized ( tracker.pendingCompletedReactiveJob ) {
                                    tracker.pendingCompletedReactiveJob.put(jobId, thisJob);
                                }
                                
                                cleanup = false;
                            } else {
                                // handover job failed. should end up entire failure.
                                if ( ! thisJob.isSpeculative() ) {
                                    // FIXME what should we do? if this is take over, either retry or halt.
                                    try {
                                        thisJob.parent.kill();
                                    } catch (InterruptedException ignore) {}
                                }
                                cleanup = true;
                            }
                        }
                    } else {
                        if ( LOG.isDebugEnabled() ) {
                            LOG.debug("Reactive reduce "+jobId+" has been "+jobStatus);
                        }
                        
                        if ("SUCCEEDED".equals(jobStatus)) {
                            thisJob.notifyParentJob(true); // complete the original
                        } else {
                            cleanOutputFile = true;
                        }
                    }
                    
                    if ( cleanOutputFile ) {
                        thisJob.parent.addPathToDelete( thisJob.getOutputPath() );
                    }
                    
                    // setup job status
                    thisJob.setState(Enum.valueOf(JobInProgress.State.class, jobStatus));
                }
                
                if ( cleanup )
                    thisJob.cleanup();
                
                resp.setStatus(HttpServletResponse.SC_OK);
            }

            resp.setContentType("text/html");
            resp.setContentLength(0);
            resp.flushBuffer();
        }
    }

    public static class SpeculationEventServlet extends HttpServlet {
        private static final long serialVersionUID = 6460188604896069661L;

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
//            String taskIdStr = req.getParameter("taskid");
//            if (taskIdStr == null)
//                return;
//            final TaskID taskId = TaskID.forName(taskIdStr);
//            String rt = req.getParameter("remainTime");
//            if ( rt != null ) {
//                float remainTime = Float.parseFloat(rt);
//            }
            /*
            String jobIdStr = req.getParameter("jobid");
            final JobID jobId = JobID.forName(jobIdStr);

            if ( LOG.isInfoEnabled() ) {
//                LOG.info("speculative taskid = "+taskId + "; remain time = "+rt);
              LOG.info("speculative execution is available for "+jobIdStr);
            }
            
            final STJobTracker tracker = (STJobTracker) getServletContext().getAttribute("job.tracker");
            if ( tracker.speculativeSplit ) {
                try {
                    // FIXME how to split it? always binary?
//                    tracker.fastSplitTask(taskId, 2);
                    tracker.scheduleSpeculativeTask(jobId);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
            
            resp.setStatus(HttpServletResponse.SC_OK);

            resp.setContentType("text/html");
            resp.setContentLength(0);
            resp.flushBuffer();
            */
        }
    }

    
    public static class SkewReportServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            String taskidStr = req.getParameter("i");
            String factorStr = req.getParameter("f");
            String offsetStr = req.getParameter("o");
            String lengthStr = req.getParameter("l");
            if (taskidStr == null || factorStr == null || offsetStr == null || lengthStr == null )
                return;
            final TaskID taskId = TaskID.forName(taskidStr);
            float factor = Float.parseFloat(factorStr);
            long offset = Long.parseLong(offsetStr);
            int  len = Integer.parseInt(lengthStr);
            
            if ( LOG.isInfoEnabled() ) {
              LOG.info("reporting a large record "+taskidStr+" factor="+factorStr+" offset="+offsetStr+" len="+lengthStr);
            }
            
            final STJobTracker tracker = (STJobTracker) getServletContext().getAttribute("job.tracker");
            final JobID jobId = taskId.getJobID();
            JobInProgress thisJob = null;
            synchronized (tracker.jobs) {
                thisJob = tracker.jobs.get(jobId);
            } 
            if ( thisJob == null ) return; // unknown
                
            TaskInProgress tip = thisJob.getTaskInProgress(taskId);
            if ( tip.addLargeRecord(factor,offset,len) ) {
                LOG.warn("task "+taskId+" has too many tasks "+ tip.getReactiveJob().getNumMapTasks() +". only "+factor+" is needed.");
            }
            
            // append the given event.
            // if factor is less than 1, do not schedule it.
            
            resp.setStatus(HttpServletResponse.SC_OK);

            resp.setContentType("text/html");
            resp.setContentLength(0);
            resp.flushBuffer();
        }
    }

    /**
     * each task attempt requested to stop and send stopping position here
     * 
     * \/split?i=[taskid]&c=[responsecode]
     * 
     * POST body is either inputsplits or minimum key
     * 
     * code
     * 
     * 0: successfully splitted -- expect the binary in the body
     * 1: can not split the task -- fall back to speculative execution
     * 2: already completed
     * 
     * @author yongchul
     *
     */
    public static class SplitTaskServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            doPost(req,resp);
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            String pi = req.getPathInfo();
            int pos = pi.lastIndexOf('/');
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("Path Info = "+pi + " pos="+pos);
            }
            
//            String attemptIdStr = req.getParameter("i");
//            String codeStr = req.getParameter("c");
            String attemptIdStr = pi.substring(1,pos);
            String codeStr = pi.substring(pos+1);
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("attempt id = "+attemptIdStr + " code = "+ codeStr);
            }
            
            if (attemptIdStr == null || codeStr == null ) {
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            final TaskAttemptID attemptId = TaskAttemptID.forName(attemptIdStr);
            final boolean isMap = attemptId.getTaskType() == TaskType.MAP;
            int code = Integer.parseInt(codeStr);
            
            if ( LOG.isInfoEnabled() ) {
              LOG.info("split response from "+attemptId+" code="+code);
            }
            
            final STJobTracker tracker = (STJobTracker) getServletContext().getAttribute("job.tracker");
            final JobID jobId = attemptId.getJobID();
            JobInProgress thisJob = null;
            synchronized (tracker.jobs) {
                thisJob = tracker.jobs.get(jobId);
            } 
            if ( thisJob == null ) {
                resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return; // unknown
            }
            
            TaskInProgress tip = thisJob.getTaskInProgress(attemptId);
            int capacity = isMap ? tracker.clusterMetrics.getMapSlotCapacity() : tracker.clusterMetrics.getReduceSlotCapacity();
            JobInProgress.ReactionContext action = null;
            boolean speculative = true;
            boolean scheduleReactive = true;
            
            int sz = req.getContentLength();
            LOG.info("available response bytes = "+sz);
            DataInputStream in = new DataInputStream( req.getInputStream() );
            
            byte[] body;
            if ( sz > (4+8) ) {
                body = new byte[sz-(4+8)];
                in.readFully(body);
            } else {
                body = new byte[0];
            }
            float tpb = in.readFloat();
            long remainBytes = in.readLong();

            if ( code == 0 ) {
                // STOP was successful. retrieve the body and split
                if ( LOG.isInfoEnabled() ) {
                    LOG.info(attemptId+" time per byte = "+tpb+" remaining bytes = "+remainBytes);
                }
                
                // if map, this is split info
                // if reduce, this is key
                if ( isMap ) {
                    // convert this current status into next thing to read
                    // FIXME construct action. split the content
                    action = new JobInProgress.TakeOverMap(tip, attemptId, body, capacity, tpb, remainBytes, code);
                } else {
//                    String enc = Base64.encodeToString(body, false);
//                    LOG.info("min reduce key = "+enc);
                    action = new JobInProgress.TakeOverReduce(tip, attemptId, body, tpb, remainBytes, code);
                }
                
                // let down stream reducers know that it should receive the reactive output as well.
                thisJob.addTaskStateEvent(tip, TaskState.TAKEOVER);
                
                speculative = false;
            } else if ( code == 1 ) {
                // can not split. fall back to speculative execution. only happens for MAP tasks
                --capacity; // leave a room for currently executing one
                action = isMap ? new ReexecMap(tip, attemptId, capacity,tpb,remainBytes) : new ReexecReduce(tip, attemptId, tpb,remainBytes);
                
                thisJob.addTaskStateEvent(tip, TaskState.CANCEL_TAKEOVER);
            } else {
                // BAD request
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }

            if ( scheduleReactive ) {
                try {
                    if ( speculative ) {
//                        tracker.fastSplitTask(attemptId.getTaskID(), capacity, action, speculative);
                        tracker.fastSplitTask(action, speculative);
                    } else {
                        PlanSpec spec = action.getPlanSpec();
                        if ( spec.requireScan() ) {
                            tracker.launchScanTask(thisJob, attemptId.getTaskID(), action);
                        } else {
    //                      tracker.fastSplitTask(attemptId.getTaskID(), capacity, action, speculative);
                            tracker.launchPlanAndLaunchTask(thisJob,attemptId.getTaskID(), action);
                        }
                    }
                } catch ( InterruptedException ex ) {
                    LOG.error(ex);
                }
            }

            // if STOP was successful, the task is the last reduce, send wait.
            
            resp.setStatus(HttpServletResponse.SC_OK);

            resp.setContentType("text/html");
            resp.setContentLength(0);
            resp.flushBuffer();
        }
    }
    
    // localize key files and etc.
    private void initializeJobDirs(String user, String jobId)
            throws IOException {
        boolean initJobDirStatus = false;
        String jobDirPath = getLocalJobDir(user, jobId);
        for (String localDir : conf.getLocalDirs()) {
            Path jobDir = new Path(localDir, jobDirPath);
            if (fs.exists(jobDir)) {
                // this will happen on a partial execution of localizeJob.
                // Sometimes copying job.xml to the local disk succeeds but
                // copying
                // job.jar might throw out an exception. We should clean up and
                // then try again.
                fs.delete(jobDir, true);
            }

            boolean jobDirStatus = fs.mkdirs(jobDir);
            if (!jobDirStatus) {
                LOG.warn("Not able to create job directory "
                        + jobDir.toString());
            }

            initJobDirStatus = initJobDirStatus || jobDirStatus;

            // job-dir has to be private to the TT
            // Localizer.PermissionsHandler.setPermissions(new
            // File(jobDir.toUri()
            // .getPath()), Localizer.PermissionsHandler.sevenZeroZero);
            // FIXME properly set permission!
        }

        if (!initJobDirStatus) {
            throw new IOException("Not able to initialize job directories "
                    + "in any of the configured local directories for job "
                    + jobId);
        }
    }

    private void scheduleJobTokenLoading(JobInProgress jip) {
        if ( jip.eventJobToken == null ) {
            synchronized (jip) {
                if ( jip.eventJobToken == null ) {
                    jip.eventJobToken = this.asyncWorkers.submit(new LoadJobTokens(jip));
                    jip.notifyAll(); // wakeup split jobs if there is any
                }
            }
        }
    }
    
    private void scheduleLoadCompletionEvents(JobInProgress jip) {
        if ( jip.eventCompletionEvents == null ) {
            synchronized ( jip ) {
                if ( jip.eventCompletionEvents == null ) {
                    jip.eventCompletionEvents = this.asyncWorkers.submit(new LoadCompletionEvents(jip));
                    jip.notifyAll();
                }
            }
            
        }
    }

    /*
     * class CopyJobTokens implements Runnable { final JobInProgress job;
     * 
     * CopyJobTokens(JobInProgress job) { this.job = job; }
     * 
     * @Override public void run() { JobID jobid = job.getDowngradeJobID(); if (
     * LOG.isInfoEnabled() ) {
     * LOG.info("copying job token file for job "+jobid.toString()); } try {
     * String jobTokenFile =
     * localizeJobTokenFile(job.getUser(),job.getJobID().toString());
     * job.setTokenStorage(TokenCache.loadTokens(jobTokenFile, conf)); } catch
     * (IOException ex) {
     * LOG.error("failed to copy job token file for job "+jobid.toString(),ex);
     * } finally { synchronized ( loadingJobToken ) {
     * loadingJobToken.remove(jobid); } } } }
     */
    
    // TODO following should run in some sort of event delivery mechanism

    class LoadInputSplitMeta implements Callable<Boolean> {
        final JobInProgress job;

        LoadInputSplitMeta(JobInProgress job) {
            this.job = job;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                JobSplit.TaskSplitMetaInfo[] metaSplits = SplitMetaInfoReader.readSplitMetaInfo(job.getJobID(), getFileSystem(), conf, job.getJobDir());
                job.setSplits(metaSplits);
            } catch (Exception e) {
                LOG.error("failed to load meta split information for job "+job.getJobID(),e);
                throw e;
            }
            return true;
        }
    }

    /**
     * Load job tokens from job submission directory. this is required process to split reduce.
     */
    class LoadJobTokens implements Callable<Boolean> {
        final JobInProgress job;

        LoadJobTokens(JobInProgress job) {
            this.job = job;
        }

        @Override
        public Boolean call() throws Exception {
            JobID jobId = job.getDowngradeJobID();
            FSDataInputStream input = null;
            TokenStorage ts = new TokenStorage();
            try {
                Path skPath = new Path(systemDir, jobId.toString()
                        + Path.SEPARATOR + TokenCache.JOB_TOKEN_HDFS_FILE);

                // FileStatus status = null;
                // long jobTokenSize = -1;
                // status = getFileSystem().getFileStatus(skPath); // throws
                // // FileNotFoundException
                // jobTokenSize = status.getLen();

                input = getFileSystem().open(skPath, 65536);
                ts.readFields(input);
                
                LOG.info("job token has been successfully loaded for "+jobId);
                job.setTokenStorage(ts);
            } catch (Exception ex) {
                LOG.error("failed to copy job token file for job " + jobId.toString(), ex);
                throw ex;
            } finally {
                if (input != null)
                    try {
                        input.close();
                    } catch (IOException ignore) {
                    }
                input = null;
            }
            return true;
        }
    }
    
    class LoadCompletionEvents implements Callable<Boolean> {
        final JobInProgress job;
        LoadCompletionEvents(JobInProgress jip) {
            this.job = jip;
        }
        
        @Override
        public Boolean call() throws Exception {
            return job.updateMapCompletionEvents();
        }
    }
    
    /**
     * Parallel speculative execution.
     * 
     * @author yongchul
     *
     */
    class SplitTask implements Callable<JobID> {
//        final JobInProgress job;
//        final TaskID taskid;
//        final int numSplits;
        final boolean speculative;
        final ReactionContext context;
        
        /*
        SplitTask(JobInProgress jip,TaskID taskid,int n) {
            this.job = jip;
            this.taskid = taskid;
            this.numSplits = n;
            this.speculative = true;
            this.action = taskid.getTaskType() == TaskType.MAP ? new JobInProgress.SplitMap(n) : null;
        }
        */
        
        /*
        SplitTask(JobInProgress jip,TaskID taskid,int n,JobInProgress.ReactionContext hook,boolean speculative) {
            this.job = jip;
            this.taskid = taskid;
            this.numSplits = n;
            this.action = hook;
            this.speculative = speculative;
        }
        */
        SplitTask(ReactionContext context,boolean speculative) {
            this.context = context;
            this.speculative = speculative;
        }
    
        @Override
        public JobID call() throws Exception {
            JobID jobid = null;
            JobInProgress job = context.getJob();
            TaskInProgress tip = context.getTaskInProgress();
            TaskID taskid = context.getTaskID();

            try {
                if ( tip.hasCommitted() ) { // last check.
                    LOG.info("task "+taskid+" has already been completed. cancel split.");
                    return null;
                }
//
//                if ( ! job.getJobID().equals(taskid.getJobID()) ) {
//                    throw new IOException("Job ID does not match: "+job.getJobID() + "/" + taskid.getJobID());
//                }
                
                long now = System.currentTimeMillis();
                ClusterInfo clusterInfo = getClusterInfo(context,now);
                Plan p = PartitionPlanner.plan(context, clusterInfo, now);
                
                LOG.debug("Splitting task "+taskid+" into "+p.getNumPartitions());
                
//                int newNumSplits = tip.adjustNumSplits(numSplits);
//                if ( numSplits != newNumSplits && LOG.isInfoEnabled() ) {
//                    LOG.info("adjusting split size from "+numSplits+" to "+newNumSplits);
//                }
                
//                TaskAttemptWithHost attemptHost = job.getSplittableTask(taskid);
//                boolean speculative = attemptHost == null;
//                if ( ! speculative ) {
//                    // set action flag
//                    tip.setAction(new TaskAction(attemptHost.getTaskAttemptID(),numSplits,false));
//                    LOG.info("accelerate "+attemptHost.getTaskAttemptID()+" by splitting remaining input into "+numSplits);
//                    newNumSplits = numSplits;
//                }
                if ( ! speculative ) {
                    // set action flag
//                    tip.setAction(new TaskAction(attemptHost.getTaskAttemptID(),numSplits,false));
//                    LOG.info("accelerate "+tip.getTaskID()+" by splitting remaining input into "+numSplits);
//                    newNumSplits = numSplits;
                }
                
                // FIXME is speculative or accelerate?
//                JobInProgress subJob = context.getJob().createReactiveJob(taskid, newNumSplits, speculative, context);
                JobInProgress subJob = context.getJob().createReactiveJob(taskid, p.getNumPartitions(), speculative, context);

//                if ( !speculative ) {
//                    int httpPort = trackerToHttpPort.get(attemptHost.getHost());
//                    String url = "http://" + attemptHost.getHost() + ":" + httpPort;
//                    subJob.getConfiguration().set(SkewTuneJobConfig.ORIGINAL_TASK_TRACKER_HTTP_ATTR,url);
//                    LOG.info("the reactive mappers will retrieve split information from "+url);
//                }
                
                subJob.getJob().submit();
                subJob.initialize();
                
                // now add subjob to the data structures
                synchronized ( jobs ) {
                    jobs.put(subJob.getDowngradeJobID(), subJob);
                }
                context.getJob().registerReactiveJob(taskid,subJob);
                jobid = subJob.getDowngradeJobID();
                
                PlannedJob plan = context.getPlan().getPlannedJob(subJob);
                
                synchronized ( plannedJobs ) {
                    plannedJobs.put(jobid,plan);
                }
                
                if ( !speculative && taskid.getTaskType() == TaskType.REDUCE ) {
                    // when reduce handover, release the original slot so that it can be reused by other reduce tasks.
                    job.markCancel(taskid);
                }
            } catch ( Exception e ) {
                LOG.error("Failed to split a job "+job.getJobID(),e);
                throw e;
            } finally {
                synchronized (pendingReactiveJob) {
                    pendingReactiveJob.remove(taskid);
                }
//                if ( !speculative ) {
                    pendingTakeOvers.unregister(taskid);
//                }
            }
            
            return jobid;
        }
    }
    
    @Override
    public String getHttpAddress() throws IOException, InterruptedException {
        return this.trackerHttp;
    }

    @Override
    public String getCompletionUrl() throws IOException, InterruptedException {
        return this.defaultNotificationUrl;
    }
    
    @Override
    public String getSpeculationEventUrl() throws IOException, InterruptedException {
        return this.defaultSpeculationEventUrl;
    }
    
    Set<TaskID> candidates = new HashSet<TaskID>();

    // continuously monitor the cluster size

    volatile ClusterMetrics clusterMetrics;
    
    class PendingTakeOverJobs {
        private Map<JobID,Set<TaskID>> taskmap = new HashMap<JobID,Set<TaskID>>();
        private int pendingTasks;

        public synchronized boolean hasPendingTasks() {
            return pendingTasks > 0;
        }
        
        public synchronized void unregister(JobID jobid) {
            Set<TaskID> tasks = taskmap.remove(jobid);
            if ( tasks != null ) {
                if ( pendingTasks < tasks.size() ) {
                    LOG.warn("pending task goes below zero: "+pendingTasks+" - "+tasks.size());
                    // recalculate
                    pendingTasks = 0;
                    for ( Set<TaskID> x : taskmap.values() ) {
                        pendingTasks += x.size();
                    }
                    LOG.warn("recomputed pending tasks = "+pendingTasks);
                } else {
                    pendingTasks -= tasks.size();
                }
            }
        }
        
        public synchronized void register(TaskID taskid) {
            JobID jobid = taskid.getJobID();
            Set<TaskID> tasks = taskmap.get(jobid);
            if ( tasks == null ) {
                tasks = new HashSet<TaskID>();
                taskmap.put(jobid, tasks);
            }
            if ( tasks.add(taskid) ) {
                ++pendingTasks;
            }
        }
        
        public synchronized void unregister(TaskID taskid) {
            JobID jobid = taskid.getJobID();
            Set<TaskID> tasks = taskmap.get(jobid);
            if ( tasks == null ) {
                LOG.warn("can't find pending take over: "+taskid);
            } else {
                if ( tasks.remove(taskid) ) {
                    --pendingTasks;
                }
            }
        }
        
        public synchronized int getNumPendingTasks() {
            return pendingTasks;
        }
    }
    

    final PendingTakeOverJobs pendingTakeOvers = new PendingTakeOverJobs();

    
    // pending speculative tasks
    class SpeculativeScheduler extends Thread {
        SpeculativeScheduler() {
            super("SpeculativeScheduler");
            setDaemon(true);
        }

        private void checkReservedTasks(int[] counts) {
            JobInProgress[] jips = null;
            synchronized (jobs) {
                jips = jobs.values().toArray(new JobInProgress[0]);
            }
            for ( JobInProgress jip : jips ) {
                jip.getNumberOfReservedTasks(counts);
            }
        }
        
        @Override
        public void run() {
            int[] reservedTasks = new int[2];
            while (true) {
                try {
                    boolean hasJobs = false;
                    synchronized (originalJobs){
                        hasJobs = ! originalJobs.isEmpty();
                    }

                    if ( hasJobs && ! pendingTakeOvers.hasPendingTasks() ) {
                        clusterMetrics = cluster.getClusterStatus();
                        reservedTasks[0] = 0;
                        reservedTasks[1] = 0;
                        
                        checkReservedTasks(reservedTasks);
                    
                        // check whether we have any idle slots
                        int availMaps = clusterMetrics.getMapSlotCapacity() - reservedTasks[0];
                        int availReduces = clusterMetrics.getReduceSlotCapacity() - reservedTasks[1];
                        
                        if ( availMaps > 0 || availReduces > 0 ) {
                            if ( LOG.isDebugEnabled() ) {
                                LOG.debug("trying to speculate tasks for map = "+availMaps+" and reduce = "+availReduces);
                            }
//                            asyncWorkers.submit(new SpeculateTask(availMaps,availReduces));
                            Set<Future<JobID>> speculated = speculateSlowest(availMaps,availReduces);
                            
                            for ( Future<JobID> fjobid : speculated ) {
                                try {
                                    JobID jobid = fjobid.get();
                                    if ( jobid == null ) {
                                        // failed to speculate
                                    } else {
                                        LOG.info("speculative job "+jobid+" has been scheduled");
                                    }
                                } catch ( ExecutionException ex ) {
                                    LOG.error("failed to retrieve job id for reactive job", ex);
                                }
                            }
                        }
                    }
                } catch ( InterruptedException x ) {
                    // silently ignore. this thread is a daemon. will reclaim itself gracefully
                } catch ( IOException iox ) {
                    LOG.error(iox);
                } catch (Exception ex) {
                    LOG.error("failed to contact to job tracker?", ex);
                } finally {
                    try {
//                      Thread.sleep(3000); // refresh every minute
                        Thread.sleep(5000); // refresh every 5 secs
                    } catch (InterruptedException ignore) {
                        break;
                    }
                }
            }
        }

        public void runOld() {
                try {
                    clusterMetrics = cluster.getClusterStatus();
                    
                    // check whether we have any idle slots
                    int availMaps = clusterMetrics.getMapSlotCapacity() - clusterMetrics.getRunningMaps();
                    int availReduces = clusterMetrics.getReduceSlotCapacity() - clusterMetrics.getRunningReduces();
                    
                    if ( (availMaps > 0 || availReduces > 0)
                            && ! pendingTakeOvers.hasPendingTasks() ) { // no take over is pending
                        boolean hasJobs = false;
                        synchronized (originalJobs){
                            hasJobs = ! originalJobs.isEmpty();
                        }
                        
                        if ( hasJobs ) {
                            if ( LOG.isDebugEnabled() ) {
                                LOG.debug("trying to speculate tasks for map = "+availMaps+" and reduce = "+availReduces);
                            }
//                            asyncWorkers.submit(new SpeculateTask(availMaps,availReduces));
                            Set<Future<JobID>> speculated = speculateSlowest(availMaps,availReduces);
                            int to = pendingTakeOvers.getNumPendingTasks();
                            if ( to > 0 ) {
                                LOG.info(to+" take over tasks have been scheduled ");
                            }
                            
                            for ( Future<JobID> fjobid : speculated ) {
                                try {
                                    JobID jobid = fjobid.get();
                                    if ( jobid == null ) {
                                        // failed to speculate
                                    } else {
                                        LOG.info("speculative job "+jobid+" has been scheduled");
                                    }
                                } catch ( ExecutionException ex ) {
                                    LOG.error("failed to retrieve job id for reactive job", ex);
                                }
                            }
                        }
                    }
                } catch ( InterruptedException ignore ) {
//                    break;
                } catch (Exception ex) {
                    LOG.error("failed to contact to job tracker?", ex);
                } finally {
                    try {
//                      Thread.sleep(3000); // refresh every minute
                        Thread.sleep(5000); // refresh every 5 secs
                    } catch (InterruptedException ignore) {
//                        break;
                    }
                }
        }

        // default
        private boolean checkSpeculation(JobInProgress job,TaskID taskid,int numSplits) {
            if ( ! job.canTakeover() ) return true;
            
            TaskAttemptWithHost attemptHost = job.getSplittableTask(taskid);
            boolean speculative = attemptHost == null;
            if ( ! speculative ) {
                // set action flag. will be fetched by next heartbeat message
                pendingTakeOvers.register(taskid);
                
                TaskInProgress tip = job.getTaskInProgress(taskid);
                tip.setAction(new TaskAction(attemptHost.getTaskAttemptID(),numSplits,false));
                job.addTaskStateEvent(tip,TaskState.PREPARE_TAKEOVER);
                LOG.info("accelerate "+attemptHost.getTaskAttemptID()+" by splitting remaining input into "+numSplits);
            }
            return speculative;
        }
        
        public Set<Future<JobID>> speculateSlowest(int availMaps,int availReduces) throws InterruptedException, IOException {
            Set<Future<JobID>> speculated = new HashSet<Future<JobID>>();
            
//            try {
            // ideally we want to use a full scheduler but let's just do a poor man's one
            HashSet<JobInProgress> jobs = new HashSet<JobInProgress>();
            synchronized (originalJobs) {
                jobs.addAll(originalJobs);
            }
            
            final int mapCapacity = clusterMetrics.getMapSlotCapacity();
            final int reduceCapacity = clusterMetrics.getReduceSlotCapacity();
            
            // do one at a time
            availMaps = availMaps > 0 ? 1 : 0;
            availReduces = availReduces > 0 ? 1 : 0;
            int scheduled = 0;
            
            HashSet<TaskID> newCandidates = new HashSet<TaskID>();
            
            long now = System.currentTimeMillis();
            
            for ( JobInProgress jip : jobs ) {
                if ( jip.doNotSpeculate ) continue;
                
//                List<STTaskStatus> tips = jip.findSpeculativeTask(availMaps,availReduces);
                List<STTaskStatus> tips = jip.findSpeculativeTaskNew(availMaps,availReduces);
                if ( LOG.isDebugEnabled() ) {
                    for ( STTaskStatus s : tips ) {
                        LOG.debug(s+": remaining time = "+s.getRemainTime(now));
                    }
                }
                
                for ( STTaskStatus tip : tips ) {
                    TaskID taskid = tip.getTaskID().getTaskID();
                    if ( ! candidates.contains(taskid) && ! jip.isRequired(taskid) ) {
                        LOG.info(taskid + " was not previously a candidate for speculative execution");
                        newCandidates.add(taskid);
                        continue;
                    }
                    
                    // now the job maybe different from the root
                    JobInProgress realJip;
                    synchronized (STJobTracker.this.jobs) {
                        realJip = STJobTracker.this.jobs.get(taskid.getJobID());
                    }
                    
                    if ( taskid.getTaskType() == TaskType.MAP ) {
//                        int maxCapacity = jip.hasCombiner() ? 3 : mapCapacity - 1;
                        int maxCapacity = mapCapacity - 1;
                        
//                        LOG.debug("HAS COMBINER? "+jip.hasCombiner());
//                        LOG.debug(jip.job.getConfiguration().get(COMBINE_CLASS_ATTR));
//                        LOG.debug(jip.job.getConfiguration().get("mapred.combiner.class"));
                        
                        if ( checkSpeculation(realJip,taskid,mapCapacity) ) {
                            // yes, it is a speculative execution
                            Future<JobID> jobid = fastSplitTask(taskid,maxCapacity);
                            if ( jobid == null ) {
                                if ( LOG.isDebugEnabled() ) {
                                    LOG.debug("task is already speculated. "+taskid);
                                }
                            } else {
                                ++scheduled;
                                speculated.add(jobid);
                            }
                            --availMaps;
                        } else {
                            // we first try to stop.
                            if ( LOG.isDebugEnabled() ) {
                                LOG.debug("handover "+taskid);
                            }
                            ++scheduled;
                            --availMaps;
                        }
                    } else {
                        // must be a reduce
                        if ( checkSpeculation(realJip,taskid,reduceCapacity) ) {
                            // speculative execution
                            Future<JobID> jobid = fastSplitTask(taskid, reduceCapacity-1);
                            if ( jobid == null ) {
                                if ( LOG.isDebugEnabled() ) {
                                    LOG.debug("task is already speculated. "+taskid);
                                }
                            } else {
                                ++scheduled;
                                speculated.add(jobid);
                            }
                            --availReduces;
                        } else { // when available, reduce will always do handover
                            if ( LOG.isDebugEnabled() ) {
                                LOG.debug("handover "+taskid);
                            }
                            if ( realJip.isReactiveJob() ) // this will start recursion in reduce job
                                scheduleLoadCompletionEvents(realJip);
                            ++scheduled;
                            --availReduces;
                        }
                    }
                }
                
                if ( availMaps == 0 && availReduces == 0 ) break;
            }
            
            candidates = newCandidates;

            if ( scheduled > 0 && LOG.isInfoEnabled() ) {
                int to = pendingTakeOvers.getNumPendingTasks();
                LOG.info(scheduled + " have been scheduled ("+to+" handovers)");
            }
            
            return speculated;
//            } catch ( Exception ex ) {
//                LOG.error(ex);
//                throw ex;
//            }
        }        
        
        
    }

    /**
     * fetch task completion events for original job and cache them for reduce split.
     * @author yongchul
     */
    class TaskCompletionEventFetcher extends Thread {
        TaskCompletionEventFetcher() {
            super("TaskCompletionEventFetcher");
            setDaemon(true);
        }
        
        @Override
        public void run() {
            LOG.info("starting task completion event fetcher");
            
            try {
                List<JobInProgress> targets = new ArrayList<JobInProgress>();
                HashSet<JobInProgress> removed = new HashSet<JobInProgress>();
                
                while ( true ) {
                    targets.clear();
                    synchronized (originalJobs) {
                        targets.addAll(originalJobs);
                    }
                    
                    if ( LOG.isTraceEnabled() && targets.size() > 0 ) {
                        LOG.trace("retrieving task completion events for "+targets);
                    }
                    
                    for ( JobInProgress jip : targets ) {
                        // check whether this has been completed or not
                        try {
                            if ( ! jip.updateTaskCompletionEvent() ) {
                                removed.add(jip);
                            }
                        } catch ( IOException ex ) {
                            LOG.error("failed to retrieve completion event for "+jip.getJobID(),ex);
                        }
                    }
                    
                    if ( ! removed.isEmpty() ) {
                        synchronized (originalJobs) {
                            originalJobs.removeAll(removed);
                        }
                        removed.clear();
                    }
                    
                    Thread.sleep(3000); // FIXME or for heartbeat interval?
                }
            } catch ( InterruptedException x ) {
                LOG.error(x);
            } catch ( Throwable x ) {
                LOG.error(x);
            }
        }
    }

    @Override
    public String getTaskTrackerURI(String tracker, String path) {
        int port = 0;
        synchronized (this) {
            TaskTrackerStatus status = trackerToLastHeartbeat.get(tracker);
            port = status.getHttpPort();
        }
        return "http://"+tracker+':'+port+path;
    }
    
    
    class ScanTask implements Callable<JobID> {
        final JobInProgress job;
        final TaskID taskid;
        final JobInProgress.ReactionContext action;
        
        ScanTask(JobInProgress jip,TaskID taskid,JobInProgress.ReactionContext hook) {
            this.job = jip;
            this.taskid = taskid;
            this.action = hook;
        }
        
        public JobInProgress getJobInProgress() { return job; }
        public TaskID getTaskID() { return taskid; }
        public JobInProgress.ReactionContext getAction() { return action; }
    
        @Override
        public JobID call() throws Exception {
            JobID jobid = null;

            try {
                if ( ! job.getJobID().equals(taskid.getJobID()) ) {
                    throw new IOException("Job ID does not match: "+job.getJobID() + "/" + taskid.getJobID());
                }
                
                if ( taskid.getTaskType() == TaskType.MAP ) {
                    // MAP
                    PartitionMapInput partMapInJob = new PartitionMapInput();
                    Job newJob = partMapInJob.prepareJob(job, taskid, action, clusterMetrics.getMapSlotCapacity());
//                    InputSplitCache.set(newJob.getConfiguration(),Collections.singletonList(tip.getInputSplit()));
                    newJob.submit();
                    
                    jobid = org.apache.hadoop.mapred.JobID.downgrade(newJob.getJobID());
                    synchronized (scanningJob) {
                        scanningJob.put(jobid, this);
                    }
                } else {
                    // REDUCE
                    PartitionMapOutput partMapOutJob = new PartitionMapOutput();
                    Job newJob = partMapOutJob.prepareJob(job, taskid, action, clusterMetrics.getReduceSlotCapacity());
                    
                    // setup appropriate notification URL. on completion, launch planning and reactive task
                    // submit it
                    newJob.submit();
                    
                    jobid = org.apache.hadoop.mapred.JobID.downgrade(newJob.getJobID());
                    
                    synchronized (scanningJob) {
                        scanningJob.put(jobid, this);
                    }
                    job.markCancel(taskid);
                }
            } catch ( Exception e ) {
                LOG.error("Failed to schedule scan task for "+taskid,e);
                throw e;
            }
            
            return jobid;
        }
    }
    
    ClusterInfo getClusterInfo(ReactionContext context,long now) throws IOException, InterruptedException {
        if ( context.getPlanSpec().requireClusterInfo() ) {
            return getClusterAvailability(context, now);
        }
        return new ClusterInfo(context.getTaskType(),clusterMetrics);
    }
    
    class PlanAndLaunchTask implements Callable<JobID> {
        final Log LOG = LogFactory.getLog(PlanAndLaunchTask.class);

        final JobInProgress job;
        final TaskID taskid;
        final JobInProgress.ReactionContext action;
        
        PlanAndLaunchTask(JobInProgress job,TaskID taskid,JobInProgress.ReactionContext hook) {
            this.job = job;
            this.taskid = taskid;
            this.action = hook;
        }
        
        public PlanAndLaunchTask(ScanTask scanTask) {
            this.job = scanTask.job;
            this.taskid = scanTask.taskid;
            this.action = scanTask.action;
        }
        
        Plan doPlan() throws IOException, InterruptedException {
            List<Partition> partitions = null;
            boolean isMap = TaskType.MAP == taskid.getTaskType();
            
            if ( action.getPlanSpec().requireScan() ) {
                if ( action.getPartitions().isEmpty() ) {
                    Path pfpath = null;
                    if ( isMap ) {
                        pfpath = PartitionMapInput.getPartitionFile(job, taskid);
                        
                        if ( LOG.isInfoEnabled() ) {
                            LOG.info("loading partition file from "+pfpath);
                        }
                        
                        // read partitionfile
                        partitions = PartitionMapInput.loadPartitionFile(fs, pfpath, conf);
                    } else {
                        pfpath = PartitionMapOutput.getPartitionFile(job, taskid);
                        
                        if ( LOG.isInfoEnabled() ) {
                            LOG.info("loading partition file from "+pfpath);
                        }
                        
                        // read partitionfile
                        partitions = PartitionMapOutput.loadPartitionFile(fs, pfpath, conf);
                    }
                } else {
                    partitions = action.getPartitions();
                    if ( LOG.isInfoEnabled() ) {
                        LOG.info("loading partitions from takeover response:"+partitions.size());
                    }
                }
                
                long totalBytes = 0;
                for ( Partition p : partitions ) {
                    totalBytes += p.getLength();
                }
                action.setRemainBytes(totalBytes);
                
                if ( LOG.isTraceEnabled() ) {
                    for ( Partition p : partitions ) {
                        LOG.trace(p);
                    }
                }
            } else {
                partitions = new ArrayList<Partition>( isMap ? clusterMetrics.getMapSlotCapacity() : clusterMetrics.getReduceSlotCapacity() );
            }
            
            long now = System.currentTimeMillis();
            ClusterInfo clusterInfo = getClusterInfo(action,now);
            
            return PartitionPlanner.plan(action, clusterInfo,partitions,now);
        }
        
        @Override
        public JobID call() throws Exception {
            JobID jobid = null;
            Plan plan = null;
            try {
                plan = doPlan();
                
                // now create subtask
                JobInProgress subJob = job.createReactiveJob(taskid, plan.getNumPartitions(), false, action);
                subJob.getJob().submit();
                subJob.initialize();
                
                // now add subjob to the data structures
                synchronized ( jobs ) {
                    jobs.put(subJob.getDowngradeJobID(), subJob);
                }
                job.registerReactiveJob(taskid,subJob);
                jobid = subJob.getDowngradeJobID();
                
                if ( taskid.getTaskType() == TaskType.REDUCE ) {
                    // when reduce handover, release the original slot so that it can be reused by other reduce tasks.
                    job.markCancel(taskid);
                }

                PlannedJob planJob = plan.getPlannedJob(subJob);
                synchronized ( plannedJobs ) {
                    plannedJobs.put( jobid, planJob );
                }
                
//                if ( taskid.getTaskType() == TaskType.REDUCE ) {
//                    scheduleJobTokenLoading(subJob);
//                }
            } catch ( Exception e ) {
                LOG.error("Failed to split a job "+job.getJobID(),e);
                throw e;
            } finally {
                synchronized (pendingReactiveJob) {
                    pendingReactiveJob.remove(taskid);
                }
                pendingTakeOvers.unregister(taskid);
            }
            
            if ( LOG.isInfoEnabled() ) {
                LOG.info("reactive job "+jobid+" has been scheduled for "+taskid);
            }
        
            return jobid;
        }
    }

    
    /**
     * schedule scan task. scan the input data and collect information.
     * once information collected, scheduling algorithm will run and launch reactive task.
     * only happens on handover
     * 
     * @param taskid
     * @param action
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private Future<JobID> scheduleScanTask(TaskID taskid,JobInProgress.ReactionContext action) throws IOException, InterruptedException {
        JobInProgress jip = null;
        synchronized (jobs) {
            jip = jobs.get(taskid.getJobID());
        }
        
        if ( jip == null ) {
            String msg = "unknown task " + taskid;
            LOG.error(msg);
            throw new IOException(msg);
        }
        
        JobID jobid = null;
        try {
            jip.waitUntilReadyToSplit(taskid);
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("scheduling asynchronous scan task for task "+taskid);
            }
            return this.asyncWorkers.submit(new ScanTask(jip, taskid, action));
        } catch (ExecutionException e) {
            throw new IOException(e.getCause()); // wrap again!
        }
    }
    
    private Future<JobID> schedulePlanAndLaunch(ScanTask scanTask) {
        return this.asyncWorkers.submit(new PlanAndLaunchTask(scanTask));
    }
    
    
    
    
    public static class SplitTaskV2Servlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            doPost(req,resp);
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            String pi = req.getPathInfo();
            int pos = pi.lastIndexOf('/');
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("Path Info = "+pi + " pos="+pos);
            }
            
//            String attemptIdStr = req.getParameter("i");
//            String codeStr = req.getParameter("c");
            String attemptIdStr = pi.substring(1,pos);
            String codeStr = pi.substring(pos+1);
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("attempt id = "+attemptIdStr + " code = "+ codeStr);
            }
            
            if (attemptIdStr == null || codeStr == null ) {
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            final TaskAttemptID attemptId = TaskAttemptID.forName(attemptIdStr);
            final boolean isMap = attemptId.getTaskType() == TaskType.MAP;
            int code = Integer.parseInt(codeStr);
            
            if ( LOG.isInfoEnabled() ) {
              LOG.info("split response from "+attemptId+" code="+code);
            }
            
            final STJobTracker tracker = (STJobTracker) getServletContext().getAttribute("job.tracker");
            final JobID jobId = attemptId.getJobID();
            JobInProgress thisJob = null;
            synchronized (tracker.jobs) {
                thisJob = tracker.jobs.get(jobId);
            } 
            if ( thisJob == null ) {
                resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return; // unknown
            }
            
            TaskInProgress tip = thisJob.getTaskInProgress(attemptId);
            int capacity = isMap ? tracker.clusterMetrics.getMapSlotCapacity() : tracker.clusterMetrics.getReduceSlotCapacity();
            JobInProgress.ReactionContext action = null;
            boolean speculative = true;
            boolean scheduleReactive = true;
            
            int sz = req.getContentLength();
            LOG.info("available response bytes = "+sz);
            DataInputStream in = new DataInputStream( req.getInputStream() );
            
            // NPARTITIONS, TPB, REMAIN BYTES
            // response code
            // 0: successful -- parallel scan [offset info][TPB][REMAINBYTES]
            // 1: can not split
            // 2: successful -- local scan (encode partition information) [p1 p2 ... pN numPartitions][TPB][REMAINBYTES]

            byte[] body;
            if ( sz > (4+8) ) {
                body = new byte[sz-(4+8)];
                in.readFully(body);
            } else {
                body = new byte[0];
            }
            float tpb = in.readFloat();
            long remainBytes = in.readLong();

            if ( code == 0 ) {
                // STOP was successful. retrieve the body and split
                if ( LOG.isInfoEnabled() ) {
                    LOG.info(attemptId+" time per byte = "+tpb+" remaining bytes = "+remainBytes);
                }
                
                // if map, this is split info
                // if reduce, this is key
                if ( isMap ) {
                    // convert this current status into next thing to read
                    // FIXME construct action. split the content
                    action = new JobInProgress.TakeOverMap(tip, attemptId, body, capacity, tpb, remainBytes, code);
                } else {
//                    String enc = Base64.encodeToString(body, false);
//                    LOG.info("min reduce key = "+enc);
                    action = new JobInProgress.TakeOverReduce(tip, attemptId, body, tpb, remainBytes, code);
                }
                
                // let down stream reducers know that it should receive the reactive output as well.
                thisJob.addTaskStateEvent(tip, TaskState.TAKEOVER);
                
                speculative = false;
            } else if ( code == 1 ) {
                // can not split. fall back to speculative execution. only happens for MAP tasks
                --capacity; // leave a room for currently executing one
                action = isMap ? new ReexecMap(tip, attemptId, capacity,tpb,remainBytes) : new ReexecReduce(tip, attemptId, tpb,remainBytes);
                
                thisJob.addTaskStateEvent(tip, TaskState.CANCEL_TAKEOVER);
            } /* else if ( code == 2 ) {
                // STOP was successful. retrieve the body and split
                if ( LOG.isInfoEnabled() ) {
                    LOG.info(attemptId+" time per byte = "+tpb+" remaining bytes = "+remainBytes);
                }
                
                // if map, this is split info
                // if reduce, this is key
                if ( isMap ) {
                    // convert this current status into next thing to read
                    // FIXME construct action. split the content
                    action = new JobInProgress.TakeOverMap(tip, attemptId, body, capacity, tpb, remainBytes, code);
                } else {
//                    String enc = Base64.encodeToString(body, false);
//                    LOG.info("min reduce key = "+enc);
                    action = new JobInProgress.TakeOverReduce(tip, attemptId, body, tpb, remainBytes, code);
                }
                
                // let down stream reducers know that it should receive the reactive output as well.
                thisJob.addTaskStateEvent(tip, TaskState.TAKEOVER);
                
                speculative = false;
            }*/ else {
                // BAD request
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }

            if ( scheduleReactive ) {
                try {
                    if ( speculative ) {
//                        tracker.fastSplitTask(attemptId.getTaskID(), capacity, action, speculative);
                        tracker.fastSplitTask(action, speculative);
                    } else {
                        PlanSpec spec = action.getPlanSpec();
                        if ( spec.requireScan()  && action.getPartitions().isEmpty() ) {
                            tracker.launchScanTask(thisJob, attemptId.getTaskID(), action);
                        } else {
    //                      tracker.fastSplitTask(attemptId.getTaskID(), capacity, action, speculative);
                            tracker.launchPlanAndLaunchTask(thisJob,attemptId.getTaskID(), action);
                        }
                    }
                } catch ( InterruptedException ex ) {
                    LOG.error(ex);
                }
            }

            // if STOP was successful, the task is the last reduce, send wait.
            
            resp.setStatus(HttpServletResponse.SC_OK);

            resp.setContentType("text/html");
            resp.setContentLength(0);
            resp.flushBuffer();
        }
    }

}
