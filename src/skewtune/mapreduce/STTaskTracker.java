package skewtune.mapreduce;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.MapReducePolicyProvider;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import skewtune.mapreduce.STJobTracker.JobCompletionServlet;
import skewtune.mapreduce.STJobTracker.SkewReportServlet;
import skewtune.mapreduce.STJobTracker.SpeculationEventServlet;
import skewtune.mapreduce.protocol.HeartbeatResponse;
import skewtune.mapreduce.protocol.JobOnTaskTracker;
import skewtune.mapreduce.protocol.MapOutputUpdates;
import skewtune.mapreduce.protocol.NewMapOutput;
import skewtune.mapreduce.protocol.ReactiveMapOutput;
import skewtune.mapreduce.protocol.STTaskStatus;
import skewtune.mapreduce.protocol.SkewTuneTaskUmbilicalProtocol;
import skewtune.mapreduce.protocol.SkewTuneTrackerProtocol;
import skewtune.mapreduce.protocol.TaskAction;
import skewtune.mapreduce.protocol.TaskStatusEvent;
import skewtune.mapreduce.protocol.TaskTrackerStatus;
import skewtune.mapreduce.server.tasktracker.TTConfig;

/**
 * SkewTune task tracker only aggregates the statistics
 * 
 * @author yongchul
 */

public class STTaskTracker 
    implements SkewTuneTaskUmbilicalProtocol, Runnable, TTConfig {

  static final long WAIT_FOR_DONE = 3 * 1000;

  static enum State {NORMAL, STALE, INTERRUPTED, DENIED}

  static{
    org.apache.hadoop.mapreduce.util.ConfigUtil.loadResources();
    skewtune.mapreduce.util.ConfigUtil.loadResources();
  }

  public static final Log LOG =
    LogFactory.getLog(STTaskTracker.class);

  public static final String MR_CLIENTTRACE_FORMAT =
    "src: %s" +     // src IP
    ", dest: %s" +  // dst IP
    ", maps: %s" + // number of maps
    ", op: %s" +    // operation
    ", reduceID: %s" + // reduce id
    ", duration: %s"; // duration

  public static final Log ClientTraceLog =
    LogFactory.getLog(STTaskTracker.class.getName() + ".clienttrace");

  volatile boolean running = true;

  private LocalDirAllocator localDirAllocator;
  String taskTrackerName;
  String localHostname;
  InetSocketAddress jobTrackAddr;
    
  InetSocketAddress taskReportAddress;

  Server taskReportServer = null;
  SkewTuneTrackerProtocol jobClient;
    
  // last heartbeat response recieved
  short heartbeatResponseId = -1;
  
  private HttpServer httpServer;
  private int httpPort;
    
  volatile boolean shuttingDown = false;
    
  /**
   * Map from taskId -> TaskInProgress.
   */
  // the information is waiting to be sent. should be included in the heartbeat.
  Map<TaskAttemptID, TaskInProgress> runningTasks;
  
  Map<JobID, RunningJob> runningJobs;
  private final JobTokenSecretManager jobTokenSecretManager 
    = new JobTokenSecretManager();

  boolean justStarted = true;
  boolean justInited = true;
  // Mark reduce tasks that are shuffling to rollback their events index
  Set<TaskAttemptID> shouldReset = new HashSet<TaskAttemptID>();
    
  static Random r = new Random();

  private JobConf fConf;

  private int maxMapSlots;
  private int maxReduceSlots;

  // MROwner's ugi
  private UserGroupInformation mrOwner;
  private String supergroup;

  private volatile boolean oobHeartbeatOnTaskCompletion;
  
  // Track number of completed tasks to send an out-of-band heartbeat
  private IntWritable finishedCount = new IntWritable(0);
  
  int workerThreads;

  /**
   * the minimum interval between jobtracker polls
   */
  private volatile int heartbeatInterval = 3000; // FIXME hard coded for now

  private void removeTaskFromJob(JobID jobId, TaskInProgress tip) {
    synchronized (runningJobs) {
      RunningJob rjob = runningJobs.get(jobId);
      if (rjob == null) {
        LOG.warn("Unknown job " + jobId + " to delete "+tip.getTaskID());
      } else {
        synchronized (rjob) {
          rjob.tasks.remove(tip);
        }
      }
    }
  }

  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException {
    if (protocol.equals(SkewTuneTaskUmbilicalProtocol.class.getName())) {
      return SkewTuneTaskUmbilicalProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol for task tracker: " +
                            protocol);
    }
  }
    
  /**
   * Do the real constructor work here.  It's in a separate method
   * so we can call it again and "recycle" the object after calling
   * close().
   */
  synchronized void initialize() throws IOException, InterruptedException {
    String keytabFilename = fConf.get(TTConfig.TT_KEYTAB_FILE);
    UserGroupInformation.setConfiguration(fConf);
    if (keytabFilename != null) {
      String desiredUser = fConf.get(TTConfig.TT_USER_NAME,
                                    System.getProperty("user.name"));
      UserGroupInformation.loginUserFromKeytab(desiredUser, 
                                               keytabFilename);
      mrOwner = UserGroupInformation.getLoginUser();
      
    } else {
      mrOwner = UserGroupInformation.getCurrentUser();
    }

    supergroup = fConf.get(MRConfig.MR_SUPERGROUP, "supergroup");
    LOG.info("Starting tasktracker with owner as " + mrOwner.getShortUserName()
             + " and supergroup as " + supergroup);

    // use configured nameserver & interface to get local hostname
    if (fConf.get(TT_HOST_NAME) != null) {
      this.localHostname = fConf.get(TT_HOST_NAME);
    }
    if (localHostname == null) {
      this.localHostname =
      DNS.getDefaultHost
      (fConf.get(TT_DNS_INTERFACE,"default"),
       fConf.get(TT_DNS_NAMESERVER,"default"));
    }
 
    // Clear out state tables
    this.runningTasks = new LinkedHashMap<TaskAttemptID, TaskInProgress>();
    this.runningJobs = new TreeMap<JobID, RunningJob>();
     
    // bind address
    InetSocketAddress socAddr = NetUtils.createSocketAddr(
        fConf.get(TT_REPORT_ADDRESS, "127.0.0.1:60100"));
    String bindAddress = socAddr.getHostName();
    int tmpPort = socAddr.getPort();
    
    // Set service-level authorization security policy
    if (this.fConf.getBoolean(
          ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      PolicyProvider policyProvider = 
        (PolicyProvider)(ReflectionUtils.newInstance(
            this.fConf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG, 
                MapReducePolicyProvider.class, PolicyProvider.class), 
            this.fConf));
      ServiceAuthorizationManager.refresh(fConf, policyProvider);
    }
    
    // RPC initialization
    int max = maxMapSlots > maxReduceSlots ? 
                       maxMapSlots : maxReduceSlots;
    //set the num handlers to max*2 since canCommit may wait for the duration
    //of a heartbeat RPC
    this.taskReportServer = RPC.getServer(this.getClass(), this, bindAddress,
        tmpPort, 2 * max, false, this.fConf, this.jobTokenSecretManager);
    this.taskReportServer.start();

    // get the assigned address
    this.taskReportAddress = taskReportServer.getListenerAddress();
    this.fConf.set(TT_REPORT_ADDRESS,
        taskReportAddress.getHostName() + ":" + taskReportAddress.getPort());
    LOG.info("TaskTracker up at: " + this.taskReportAddress);

    this.taskTrackerName = "tracker_" + localHostname + ":" + taskReportAddress;
    LOG.info("Starting tracker " + taskTrackerName);
    

    // HTTP server initialization
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(fConf.get(
            TT_HTTP_ADDRESS, String.format("%s:0",localHostname)));
    String infoBindAddress = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    httpServer = new HttpServer("task", infoBindAddress, tmpInfoPort,
            tmpInfoPort == 0, fConf);
    httpServer.setAttribute("task.tracker", this);
    httpServer.addServlet("taskprogress", "/progress", TaskProgressServlet.class);
    httpServer.start();

    // The rpc/web-server ports can be ephemeral ports...
    // ... ensure we have the correct info
    this.httpPort = this.httpServer.getPort();
    this.fConf.set(TT_HTTP_ADDRESS, infoBindAddress + ":" + this.httpPort);
    LOG.info("TaskTracker webserver: " + this.httpServer.getPort());


    this.jobClient = (SkewTuneTrackerProtocol) 
    mrOwner.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException {
        return RPC.waitForProxy(SkewTuneTrackerProtocol.class,
            SkewTuneTrackerProtocol.versionID, 
            jobTrackAddr, fConf);  
      }
    }); 
    this.justInited = true;
    this.running = true;    

    oobHeartbeatOnTaskCompletion =
        fConf.getBoolean(TT_OUTOFBAND_HEARTBEAT, false);
  }

  UserGroupInformation getMROwner() {
    return mrOwner;
  }

  String getSuperGroup() {
    return supergroup;
  }
  
  /**
   * Is job level authorization enabled on the TT ?
   */
  boolean isJobLevelAuthorizationEnabled() {
    return fConf.getBoolean(
        MRConfig.JOB_LEVEL_AUTHORIZATION_ENABLING_FLAG, false);
  }

  // Object on wait which MapEventsFetcherThread is going to wait.
  private Object waitingOn = new Object();

  private static LocalDirAllocator lDirAlloc = 
                              new LocalDirAllocator(MRConfig.LOCAL_DIR);
     
  public synchronized void shutdown() throws IOException {
    shuttingDown = true;
    close();
  }
  
  /**
   * Close down the TaskTracker and all its components.  We must also shutdown
   * any running tasks or threads, and cleanup disk space.  A new TaskTracker
   * within the same process space might be restarted, so everything must be
   * clean.
   */
  public synchronized void close() throws IOException {
    this.running = false;

    // shutdown RPC connections
    RPC.stopProxy(jobClient);

    if (taskReportServer != null) {
        taskReportServer.stop();
        taskReportServer = null;
    }
    
    if ( httpServer != null ) {
        try {
            httpServer.stop();
        } catch ( Exception e ) {
            throw new IOException(e);
        }
        httpServer = null;
    }
  }

  /**
   * For testing
   */
  STTaskTracker() {
    httpServer = null;
  }

  void setConf(JobConf conf) {
    fConf = conf;
  }

  /**
   * Start with the local machine name, and the default JobTracker
   */
  public STTaskTracker(JobConf conf) throws IOException, InterruptedException {
    fConf = conf;
    maxMapSlots = conf.getInt(TT_MAP_SLOTS, 2);
    maxReduceSlots = conf.getInt(TT_REDUCE_SLOTS, 2);
    this.jobTrackAddr = STJobTracker.getAddress(conf);
    // let the jsp pages get to the task tracker, config, and other relevant
    // objects
    FileSystem local = FileSystem.getLocal(conf);
    this.localDirAllocator = new LocalDirAllocator(MRConfig.LOCAL_DIR);
    initialize();
  }

  /** Return the port at which the tasktracker bound to */
  public synchronized InetSocketAddress getTaskTrackerReportAddress() {
    return taskReportAddress;
  }

  /**
   * Main service loop.  Will stay in this loop forever.
   */
  State offerService() throws Exception {
    long lastHeartbeat = 0;

    while (running && !shuttingDown) {
      try {
        long now = System.currentTimeMillis();

        long waitTime = heartbeatInterval - (now - lastHeartbeat);
        if (waitTime > 0) {
          // sleeps for the wait time or
          // until there are empty slots to schedule tasks
          synchronized (finishedCount) {
            if (finishedCount.get() == 0) {
              finishedCount.wait(waitTime);
            }
            finishedCount.set(0);
          }
        }

        // If the TaskTracker is just starting up:
        // 1. Verify the buildVersion
        // 2. Get the system directory & filesystem
        if(justInited) {
            /*
          String jobTrackerBV = jobClient.getBuildVersion();
          if(!VersionInfo.getBuildVersion().equals(jobTrackerBV)) {
            String msg = "Shutting down. Incompatible buildVersion." +
            "\nJobTracker's: " + jobTrackerBV + 
            "\nTaskTracker's: "+ VersionInfo.getBuildVersion();
            LOG.error(msg);
            try {
              jobClient.reportTaskTrackerError(taskTrackerName, null, msg);
            } catch(Exception e ) {
              LOG.info("Problem reporting to jobtracker: " + e);
            }
            return State.DENIED;
          }
          */
        }
        
        // Send the heartbeat and process the jobtracker's directives
        HeartbeatResponse heartbeatResponse = transmitHeartBeat(now);

        // Note the time when the heartbeat returned, use this to decide when to send the
        // next heartbeat   
        lastHeartbeat = System.currentTimeMillis();
        
        ReactiveMapOutput[] newMapOutput = heartbeatResponse.getNewMapOutputs();
//        TaskAttemptID[] cancelled = heartbeatResponse.getCancelledTasks();
        TaskAction[] actions = heartbeatResponse.getTaskActions();
        JobID[] unknowns = heartbeatResponse.getUnknownJobs();
        TaskStatusEvent[] takeovers = heartbeatResponse.getTakeovers();

        if (LOG.isDebugEnabled() &&
                ((newMapOutput != null && newMapOutput.length > 0)
                        || (actions != null && actions.length > 0)
                        || (unknowns != null && unknowns.length > 0)
                        || (takeovers != null && takeovers.length > 0) ) ) {
          LOG.debug("Got heartbeatResponse from JobTracker with responseId: " + 
                    heartbeatResponse.getResponseId() + " and " + 
                    (( newMapOutput != null) ? newMapOutput.length : 0) + " new map outputs and " +
//                    (( cancelled != null) ? cancelled.length : 0) + " cancel msgs " +
                    (( actions != null) ? actions.length : 0) + " action msgs " +
                    (( unknowns != null) ? unknowns.length : 0) + " unknown msgs "+
                    (( takeovers != null ) ? takeovers.length : 0 ) + " takeover msgs");
        }
            
        // resetting heartbeat interval from the response.
        heartbeatInterval = heartbeatResponse.getHeartbeatInterval();
        justStarted = false;
        justInited = false;

        // first assign new map outputs
        if ( newMapOutput.length > 0 ) {
            synchronized ( runningJobs ) {
                for ( ReactiveMapOutput newOutput : newMapOutput ) {
                    JobID jobid = newOutput.getJobID();
                    RunningJob rjob = runningJobs.get(jobid);
                    if ( rjob == null ) {
                        LOG.warn("Unknown job id: "+jobid);
                    } else {
                        rjob.addNewMapOutput(newOutput);
                        LOG.info("adding a new map output for job "+jobid+ " partition "+newOutput.getPartition());
                    }
                }
            }
        }

        // then we mark cancelled tasks
//        if ( cancelled.length > 0 ) {
//            markCancelledTasks(cancelled);
//        }
        if ( actions.length > 0 ) {
            setTaskActions(actions);
        }
        
        if ( takeovers.length > 0 ) {
            setTakeovers(takeovers);
        }

        // first purge jobs that are cancelled.
        for ( JobID jobid : unknowns ) {
            LOG.info("purging job "+jobid);
            purgeJob(jobid);
        }
     } catch (InterruptedException ie) {
        LOG.info("Interrupted. Closing down.");
        return State.INTERRUPTED;
      } catch (RemoteException re) {
        String reClass = re.getClassName();
//        if (DisallowedTaskTrackerException.class.getName().equals(reClass)) {
//          LOG.info("Tasktracker disallowed by JobTracker.");
//          return State.DENIED;
//        }
        LOG.info("Remote exception?",re);
      } catch (Exception except) {
        String msg = "Caught exception: " + 
          StringUtils.stringifyException(except);
        LOG.error(msg);
      }
    }

    return State.NORMAL;
  }

  private long previousUpdate = 0;

  private TaskTrackerStatus status;

  /**
   * Build and transmit the heart beat to the JobTracker
   * @param now current time
   * @return false if the tracker was unknown
   * @throws IOException
   */
  HeartbeatResponse transmitHeartBeat(long now) throws IOException, InterruptedException {
    // 
    // Check if the last heartbeat got through... 
    // if so then build the heartbeat information for the JobTracker;
    // else resend the previous status information.
    //

      // FIXME generate task status
    if (status == null) {
      synchronized (this) {
        status = new TaskTrackerStatus(taskTrackerName,
                                       localHostname, 
                                       httpPort,
                                       cloneTaskInProgressStatuses()
                                       );
      }
    } else {
      LOG.info("Resending 'status' to '" + jobTrackAddr.getHostName() +
               "' with reponseId '" + heartbeatResponseId);
      // LOG this message
      FileSystem fs = FileSystem.getLocal(fConf).getRaw();
      Path p = new Path(String.format("/tmp/heartbeat-%d" , heartbeatResponseId));
      if ( ! fs.exists(p) ) {
          FSDataOutputStream out = fs.create(p);
          status.write(out);
          out.close();
      }
    }
      
    //
    // Xmit the heartbeat
    //
    HeartbeatResponse heartbeatResponse = jobClient.heartbeat(status, 
                                                              justStarted,
                                                              justInited,
                                                              heartbeatResponseId);
      
    //
    // The heartbeat got through successfully!
    //
    heartbeatResponseId = heartbeatResponse.getResponseId();

    // Force a rebuild of 'status' on the next iteration
    status = null;                                

    return heartbeatResponse;
  }

  private void notifyTTAboutTaskCompletion() {
      if ( oobHeartbeatOnTaskCompletion ) {
          synchronized (finishedCount) {
              int value = finishedCount.get();
              finishedCount.set(value+1);
              finishedCount.notify();
          }
      }
  }


  /**
   * The server retry loop.  
   * This while-loop attempts to connect to the JobTracker.  It only 
   * loops when the old TaskTracker has gone bad (its state is
   * stale somehow) and we need to reinitialize everything.
   */
  public void run() {
    try {
      boolean denied = false;
      while (running && !shuttingDown && !denied) {
        boolean staleState = false;
        try {
          // This while-loop attempts reconnects if we get network errors
          while (running && !staleState && !shuttingDown && !denied) {
            try {
              State osState = offerService();
              if (osState == State.STALE) {
                staleState = true;
              } else if (osState == State.DENIED) {
                denied = true;
              }
            } catch (Exception ex) {
              if (!shuttingDown) {
                LOG.info("Lost connection to JobTracker [" +
                         jobTrackAddr + "].  Retrying...", ex);
                try {
                  Thread.sleep(5000);
                } catch (InterruptedException ie) {
                }
              }
            }
          }
        } finally {
          close();
        }
        if (shuttingDown) { return; }
        LOG.warn("Reinitializing local state");
        initialize();
      }
      if (denied) {
        shutdown();
      }
    } catch (IOException iex) {
      LOG.error("Got fatal exception while reinitializing TaskTracker: " +
                StringUtils.stringifyException(iex));
      return;
    }
    catch (InterruptedException i) {
      LOG.error("Got interrupted while reinitializing TaskTracker: " + 
          i.getMessage());
      return;
    }
  }
 
  /**
   * Get the name for this task tracker.
   * @return the string like "tracker_mymachine:50010"
   */
  String getName() {
    return taskTrackerName;
  }

  private synchronized List<JobOnTaskTracker> cloneTaskInProgressStatuses() {
      RunningJob[] rjobs = new RunningJob[0];
      
      synchronized ( runningJobs ) {
          rjobs = runningJobs.values().toArray(rjobs);
      }

      List<JobOnTaskTracker> result = new ArrayList<JobOnTaskTracker>(rjobs.length);
      List<STTaskStatus> taskStatus = new ArrayList<STTaskStatus>();
      
      for ( RunningJob rjob : rjobs ) {
          int from = -1;
          int fromTakeover = -1;
          
          synchronized (rjob) {
              taskStatus.clear();
              from = rjob.getNumAvailMapOutput();
              fromTakeover = rjob.getCurrentIndexOfTakeoverMaps();
              for ( TaskInProgress tip : rjob ) {
                  taskStatus.add( (STTaskStatus)tip.getStatus().clone() );
              }
          }
          
          result.add( new JobOnTaskTracker(rjob.getJobID(),from,taskStatus,fromTakeover) );
      }

      return result;
  }

  /**
   * Get the default job conf for this tracker.
   */
  JobConf getJobConf() {
    return fConf;
  }
    
  /**
   * Start the TaskTracker, point toward the indicated JobTracker
   */
  @SuppressWarnings("deprecation")
public static void main(String argv[]) throws Exception {
    StringUtils.startupShutdownMessage(STTaskTracker.class, argv, LOG);
    if (argv.length != 0) {
      System.out.println("usage: TaskTracker");
      System.exit(-1);
    }
    try {
      JobConf conf=new JobConf();
      // enable the server to track time spent waiting on locks
      ReflectionUtils.setContentionTracing
        (conf.getBoolean(TT_CONTENTION_TRACKING, false));
      new STTaskTracker(conf).run();
    } catch (Throwable e) {
      LOG.error("Can not start task tracker because "+
                StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
  
    class TaskInProgress {
        private final TaskAttemptID taskid;
        private final STTaskStatus taskStatus;
        private volatile long lastReport; // last call of statusUpdate()
        private volatile boolean done;    // set by RPC. cleared by heartbeat logic.
        private volatile boolean cancel;
        private volatile int  action;
        private Object splitInfo; // byte[] for reduce, ??? for map

        TaskInProgress(TaskAttemptID id) {
            this.taskid = id;
            this.taskStatus = new STTaskStatus(this.taskid);
        }

        boolean isMap() {
            return taskid.getTaskType() == TaskType.MAP;
        }

        JobID getJobID() {
            return taskid.getJobID();
        }

        TaskAttemptID getTaskID() {
            return taskid;
        }

        synchronized STTaskStatus getStatus() {
            return taskStatus;
        }
        
        synchronized void setSplitInfo(Object obj) {
            splitInfo = obj;
            notifyAll();
        }
        
        synchronized Object getSplitInfo(long to) throws InterruptedException {
            while ( splitInfo == null ) {
                wait(to);
            }
            return splitInfo;
        }
        
        void setAction(int action) {
            this.action = action;
        }
        int getAction() { return action; }
        
        void cancel() { cancel = true; }

        synchronized void reportProgress(STTaskStatus status) {
            if ( this.done || 
                    this.taskStatus.getRunState() != TaskStatus.State.RUNNING ) {
                LOG.info(taskid + " Ignoring status-update since " +
                        ((this.done) ? "task is 'done'" :
                         ("runState: "+ this.taskStatus.getRunState())));
                return;
            }
            this.taskStatus.statusUpdate(status);
//            LOG.debug("remain time = "+status.getRemainTime());
            this.lastReport = System.currentTimeMillis();
        }

        synchronized void reportDone() {
            // FIXME
            this.taskStatus.setRunState(TaskStatus.State.SUCCEEDED);
            this.taskStatus.setProgress(1.0f);
            this.taskStatus.setRemainTime(0, 0);
            this.done = true;
        }

        boolean isDone()  { return done; }
        boolean isCancelled() { return (action & TaskAction.FLAG_CANCEL) != 0; }

        long getLastReport() {
            return lastReport;
        }
    }
  
    class RunningJob implements Iterable<TaskInProgress> {
        final JobID jobid;
        final int numMap;
        final int numReduce;

        ReactiveMapOutput[] newMapOutput; // initialized only when necessary
        int   numMapOutput;
        
        ArrayList<TaskStatusEvent.Internal> takeOverMaps;

        HashSet<TaskInProgress> tasks;
        int numActiveReduces;

        RunningJob(JobID jobid,int numMap,int numReduce) {
            this.jobid = jobid;
            this.numMap = numMap;
            this.numReduce = numReduce;
            tasks = new HashSet<TaskInProgress>();
            takeOverMaps = new ArrayList<TaskStatusEvent.Internal>();
        }

        synchronized void addTask(TaskInProgress task) {
            tasks.add(task);
            if ( !task.isMap() ) {
                ++numActiveReduces;
            }
        }

        synchronized void removeTask(TaskInProgress task) {
            tasks.remove(task);
            if ( ! task.isMap() ) {
                --numActiveReduces;
            }
        }

        synchronized void clear() {
            tasks.clear();
            numActiveReduces = 0;
        }

        JobID getJobID() { return jobid; }
        int getNumMaps() { return numMap; }
        int getNumReduces() { return numReduce; }
        int getNumActiveReduces() { return numActiveReduces; }
        int getNumAvailMapOutput() {
            return ( numReduce > 0 && numMapOutput < numMap ) ? numMapOutput : -1;
        }
        
        @Override
        public Iterator<TaskInProgress> iterator() {
            return tasks.iterator();
        }

        synchronized int addNewMapOutput(ReactiveMapOutput output) {
            if ( newMapOutput == null ) {
                newMapOutput = new ReactiveMapOutput[numMap];
            }
            newMapOutput[numMapOutput++] = output;
            return numMapOutput;
        }
        
        synchronized int getCurrentIndex() {
            return numMapOutput;
        }
        
        synchronized int addNewTakeoverMap(TaskStatusEvent e) {
            takeOverMaps.add(e.toInternal());
            return takeOverMaps.size();
        }
        
        synchronized int getCurrentIndexOfTakeoverMaps() {
            return takeOverMaps.size();
        }

        synchronized MapOutputUpdates getNewMapOutputs(int reduce,int from,int fromTakeover) {
            int limit = getCurrentIndex();
            int limitTakeover = getCurrentIndexOfTakeoverMaps();
            NewMapOutput[] newMapOuts = MapOutputUpdates.EMPTY_NEW_MAP_OUTPUT;
            TaskStatusEvent.Internal[] newTakeovers = MapOutputUpdates.EMPTY_NEW_TAKEOVER_MAP;
            
            if ( from < limit ) {
                final int sz = limit - from;
                newMapOuts = new NewMapOutput[sz];
                for ( int i = 0; i < sz; ++i ) {
                    ReactiveMapOutput totalOutput = newMapOutput[from++];
                    newMapOuts[i] = new NewMapOutput(totalOutput,reduce);
                }
            }
            if ( fromTakeover < limitTakeover ) {
                final int sz = limitTakeover - fromTakeover;
                newTakeovers = new TaskStatusEvent.Internal[sz];
                for ( int i = 0; i < sz; ++i ) {
                    newTakeovers[i] = takeOverMaps.get(fromTakeover++);
                }
            }
            return new MapOutputUpdates(newMapOuts,newTakeovers);
        }
    }

    @Override
    public synchronized int init(TaskAttemptID taskId,
            int numMap,int numReduce)
    throws IOException {
        JobID jobid = taskId.getJobID();
        TaskInProgress task = new TaskInProgress(taskId);
        RunningJob rjob = null;
        boolean isKnown;

        runningTasks.put(task.getTaskID(),task);
        synchronized (runningJobs) {
            isKnown = runningJobs.containsKey(jobid);
            if ( isKnown ) {
                rjob = runningJobs.get(jobid);
            } else {
                rjob = new RunningJob(jobid, numMap, numReduce);
                runningJobs.put(jobid,rjob);
            }
            synchronized (rjob) {
                rjob.addTask(task);
            }
        }
        
        LOG.info(taskId.toString() + " has been initialized");
        
        return isKnown ? 0 : TaskAction.FLAG_CANCEL;
    }

    @Override
    public synchronized int ping(TaskAttemptID taskId)
    throws IOException {
        TaskInProgress tip = runningTasks.get(taskId);
        return tip == null ? TaskAction.FLAG_CANCEL : tip.getAction();
    }

    @Override
    public synchronized int statusUpdate(
            TaskAttemptID taskid, STTaskStatus status)
    throws IOException {
        TaskInProgress tip = runningTasks.get(taskid);
        if ( tip != null ) {
            tip.reportProgress(status);
            return tip.getAction();
        } else {
            LOG.warn("Progress from unknown child task: "+taskid);
            return TaskAction.FLAG_CANCEL;
        }
    }

    @Override
    public synchronized int done(TaskAttemptID taskId) throws IOException {
        TaskInProgress tip = runningTasks.get(taskId);
        if ( tip == null ) {
            LOG.warn("Done for unknown task: "+taskId.toString());
            return TaskAction.FLAG_CANCEL;
        } else {
            tip.reportDone();
            notifyTTAboutTaskCompletion();
            LOG.info(taskId.toString() + " is done");
            return tip.getAction();
        }
    }

    @Override
    public MapOutputUpdates getCompltedMapOutput(
            TaskAttemptID taskid, int from, int fromTakeover)
            throws IOException {
        JobID jobid = taskid.getJobID();
        MapOutputUpdates updates = MapOutputUpdates.EMPTY_UPDATE;
        RunningJob rjob = null;
        synchronized (runningJobs) {
            rjob = runningJobs.get(jobid);
        }
        if ( rjob != null ) {
//          synchronized (rjob) {
            // following operations are all read only.
            updates = rjob.getNewMapOutputs(taskid.getTaskID().getId(),from,fromTakeover);
//          }
            if ( updates.size() > 0 ) {
                LOG.info(jobid.toString() + " will process "+updates.size()+" new reactive map ouptuts");
            }
        } else {
            LOG.warn("Requesting map output for unknown job: "+jobid);
        }
        return updates;
    }

    private void purgeJob(JobID jobid)
        throws IOException {
        RunningJob rjob = null;
        synchronized (runningJobs) {
            rjob = runningJobs.remove(jobid);
        }
        if ( rjob == null ) {
            LOG.info("job is being deleted: "+jobid);
        } else {
            synchronized (rjob) {
                for ( TaskInProgress task : rjob.tasks ) {
                    runningTasks.remove(task.getTaskID());
                    LOG.info("purging task "+task.getTaskID());
                }
                rjob.clear(); // we are told to clear everything.
            }
        }
    }

    synchronized void markCancelledTasks(TaskAttemptID[] tasks) {
        for ( TaskAttemptID taskid : tasks ) {
            TaskInProgress tip = runningTasks.get(taskid);
            if ( tip != null ) {
                tip.cancel();
                LOG.info("cancel task "+taskid);
            }
        }
    }
    
    synchronized void setTaskActions(TaskAction[] actions) {
        for ( TaskAction action : actions ) {
            TaskInProgress tip = runningTasks.get(action.getAttemptID());
            if ( tip != null ) {
                tip.setAction(action.getAction());
                LOG.info("task action "+action);
            }
        }
    }

    synchronized void setTakeovers(TaskStatusEvent[] takeovers) {
        JobID prevJobId = null;
        RunningJob jip = null;
        
        for ( TaskStatusEvent event : takeovers ) {
            JobID jobid = event.getJobID();
            synchronized (runningJobs) {
                jip = runningJobs.get(jobid);
            }
            if ( jip != null ) {
                jip.addNewTakeoverMap(event);
            }
        }
    }

//    private void purgeTask(TaskInProgress task,boolean wasFailure)
//    throws IOException {
//        if ( task != null ) {
//            LOG.info("About to purge task: " + task.getTaskID());
//            removeTaskFromJob(task.getJobID(),task);
//        }
//    }
    
    /**
     * Query scheme
     * 
     * /progress?i=taskAttemptId
     * 
     * Response code
     * 200: good
     * 404: no such tasks -- probably completed or killed already
     */
    
    public static class TaskProgressServlet extends HttpServlet {
        /**
         * 
         */
        private static final long serialVersionUID = -6331603117723983592L;

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            String taskidStr = req.getParameter("i");
            String requestidStr = req.getParameter("m"); // task id of requester
            if (taskidStr == null || requestidStr == null ) {
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                return;
            }
            
            final TaskID taskid = TaskID.forName(taskidStr);
            final int reqTaskID = Integer.parseInt(requestidStr);
            if ( LOG.isInfoEnabled() ) {
              LOG.info("checking progress of "+taskid+" from task "+reqTaskID);
            }
            
            final STTaskTracker tracker = (STTaskTracker) getServletContext().getAttribute("task.tracker");
            RunningJob thisJob = null;
            synchronized (tracker.runningJobs) {
                thisJob = tracker.runningJobs.get(taskid.getJobID());
            }
            
            if ( thisJob == null ) {
                resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return; // unknown
            }

            TaskInProgress thisTask = null;
            synchronized (thisJob) {
                for ( TaskInProgress tip : thisJob.tasks ) {
                    if ( taskid.equals(tip.getTaskID().getTaskID()) ) {
                        thisTask = tip;
                        break;
                    }
                }
            }
            
            if ( thisTask == null ) {
                resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
                return; // unknown
            }
            
            Object splitInfo = null;
            try {
                splitInfo = thisTask.getSplitInfo(1000);
            } catch (InterruptedException e) {
                resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                return;
                
            } // wait up to 1 seconds
            
            if ( splitInfo == null ) {
                resp.setStatus(HttpServletResponse.SC_ACCEPTED);
                return;
            }

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.setContentType("application/octet-stream");

            ServletOutputStream out = resp.getOutputStream();
            if ( thisTask.isMap() ) {
                MapSplitIndex index = (MapSplitIndex)splitInfo;
                int len = index.getLength(reqTaskID);
                resp.setContentLength(len);
                index.writeTo(out, reqTaskID);
            } else {
                byte[] binkey = (byte[])splitInfo;
                resp.setContentLength(binkey.length);
                out.write(binkey);
            }
            
            resp.flushBuffer();
        }
    }

    @Override
    public synchronized void setSplitInfo(TaskAttemptID taskId, byte[] data) throws IOException {
        TaskInProgress tip = runningTasks.get(taskId);
        if ( tip == null ) {
            LOG.warn("Done for unknown task: "+taskId.toString());
        } else {
            // setup the data -- parse it if map. if reduce, it's the key
            
            if ( taskId.getTaskType() == TaskType.MAP ) {
                // this is collection of inputsplit
                MapSplitIndex index = new MapSplitIndex(data);
                tip.setSplitInfo(index);
                
                if ( LOG.isInfoEnabled() ) {
                    LOG.info("split "+taskId+": map split index = "+index.numSplits+" "+data.length+" bytes");
                }
            } else {
                // this is the reduce key
                tip.setSplitInfo(data);
                
                if ( LOG.isInfoEnabled() ) {
                    LOG.info("split "+taskId+": reduce key = "+Arrays.toString(data));
                }
            }
        }
    }
    
    class MapSplitIndex {
        final int numSplits;
        final byte[] info;
        
        // i th element in index contains the offset of last bytes of i th data item.
        // length is calculated by subtracting previous item
        final ByteBuffer index;
        
        MapSplitIndex(byte[] data) {
            info = data;
            
            numSplits = ((data[data.length - 4]&0xff) << 24)
                    | ((data[data.length -3] & 0xff) << 16)
                    | ((data[data.length -2] & 0xff) << 8)
                    | ((data[data.length - 1] & 0xff));
            int len = numSplits << 2;
            int off = data.length - 4 - len;
            index = ByteBuffer.wrap(data, off, len);
        }
        
        public int getLength(int idx) {
            int off = idx == 0 ? 0 : index.getInt(idx-1);
            return index.getInt(idx) - off;
        }
        
        public int writeTo(OutputStream out,int idx) throws IOException {
            int off = idx == 0 ? 0 : index.getInt(idx-1);
            int len = index.getInt(idx) - off;
            out.write(info, off, len);
            return len;
        }
    }
}
