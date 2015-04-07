

# Prerequisite Software #

  * Apache Ant (http://ant.apache.org/)
  * Hadoop 0.21.0 (jar files are included in the distribution)

# How to download? #

  * Check out the git repository with following command line.
```
git clone https://code.google.com/p/skewtune/
```
  * Or, download the tar ball from the [download section](http://code.google.com/p/skewtune/downloads/list).

# How to build? #

run ant.

```
% cd skewtune
% ant
```

# How to install? #

The distribution includes patched scripts and jars of Hadoop. Thus, you can simply overwrite existing Hadoop 0.21.0 installation.

  1. stop running Hadoop MapReduce and HDFS services
  1. overwrite the existing Hadoop installation
  1. start HDFS
  1. start MapReduce
  1. start SkewTune

The first step is necessary since the SkewTune distribution includes patches to HDFS data node, Job Tracker, and Task Tracker. For the list of changes, please refer SkewTuneHadoopDiff wiki.

**NOTE:** Backup your configurations and scripts before overwrite.

## Configurations ##

As other Hadoop daemons, SkewTune will override default configuration if it can find ```
skewtune-site.xml``` at a known location (typically where all the &quot;site&quot; configuration files are).

The one thing you should override is the ```
skewtune.jobtracker.address```. If possible, please spell out the IP address of the job tracker machine.

For the detail of configuration, please check SkewTuneConfiguration.

# How to start/stop SkewTune daemons? #

Starting and stopping SkewTune is similar to that of Hadoop MapReduce.

```
% bin/start-skewtune.sh
```

To stop SkewTune, issue following command line.
```
% bin/stop-skewtune.sh
```

**NOTE:** HDFS and Hadoop MapReduce must be running before.

# How to start a job? #

A job must be submitted through the SkewTune client interface
(skewtune.mapreduce.Job).  The client interface closely resembles the Hadoop job submission interface (i.e., you can use the same API to construct a JobConf and submit). For example, following will run your Hadoop mapreduce job in old API with SkewTune.

```
org.apache.hadoop.mapred.JobConf yourJobConf;

// ...
// ... configure your job as usual
// ...

skewtune.mapreduce.SkewTuneJob stJob = skewtune.mapreduce.SkewTuneJob.getInstance(yourJobConf);
stJob.submit();
stJob.waitForCompletion(true);
```

You can also supply ```
org.apache.hadoop.mapreduce.Job``` object to ```
SkewTuneJob.getInstance()``` method to run an application written in the new API.

```
hadoop jar``` style command line is not supported yet.


## Configurations ##

TBD

# Further Readings #

For further references, please refer following documents and demo.

  * [SkewTune: Mitigating Skew in MapReduce Applications](http://dl.acm.org/citation.cfm?id=2213840) [Extended Version](ftp://ftp.cs.washington.edu/tr/2012/03/UW-CSE-12-03-03.PDF)
  * VLDB 2012 Demo [Proposal](http://vldb.org/pvldb/vol5/p1934_yongchulkwon_vldb2012.pdf) [Demo](Demo.md)
  * Ph.D. Chapter 5. Managing Skew in ...  Ph.D. dissertation
