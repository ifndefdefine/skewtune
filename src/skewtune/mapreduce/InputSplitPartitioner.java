package skewtune.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import skewtune.mapreduce.PartitionMapInput.MapInputPartition;
import skewtune.mapreduce.PartitionPlanner.Partition;
import skewtune.mapreduce.PartitionPlanner.Range;

/**
 * Split given InputSplit object into multiple splits. Currently only support
 * FileSplit and CombineFileSplit
 * 
 */
public class InputSplitPartitioner implements SkewTuneJobConfig {
    private static final Log LOG = LogFactory
            .getLog(InputSplitPartitioner.class);

    /**
     * By default, always halve the split
     */
    public static final int DEFAULT_SPLITS = 2;
    
//    private final static long MEGABYTES = 1024 * 1024;


    public static List<org.apache.hadoop.mapreduce.InputSplit> split(
            Configuration job, org.apache.hadoop.mapreduce.InputSplit s)
            throws IOException {
        return split(job, s, DEFAULT_SPLITS);
    }

    public static List<org.apache.hadoop.mapreduce.InputSplit> split(
            Configuration job, org.apache.hadoop.mapreduce.InputSplit s, int n)
            throws IOException {
        if (s instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
            return split(job, (org.apache.hadoop.mapreduce.lib.input.FileSplit) s, n);
        } else if (s instanceof org.apache.hadoop.mapreduce.lib.input.CombineFileSplit) {
            return split(job, (org.apache.hadoop.mapreduce.lib.input.CombineFileSplit) s, n);
        }
        throw new IOException("Unsupported input split type: " + s.getClass());
    }


    private static List<org.apache.hadoop.mapreduce.InputSplit> split(
            Configuration conf, org.apache.hadoop.mapreduce.lib.input.FileSplit s, int n) throws IOException {
        if (LOG.isInfoEnabled())
            LOG.info("Split filesplit (" + s + ") into " + n);

        Path fileName = s.getPath();
        long startOffset = s.getStart();
        long remain = s.getLength();
        
        // EXTRACT HOST INFORMATION
        HashSet<String> hosts = new HashSet<String>();
        FileSystem fs = fileName.getFileSystem(conf);
//        FileStatus fileStatus = fs.getFileStatus(fileName);
//        BlockLocation[] blocs = fs.getFileBlockLocations(fileStatus, startOffset, remain);
        BlockLocation[] blocs = fs.getFileBlockLocations(fileName, startOffset, remain);
        for ( BlockLocation blk : blocs ) {
            for ( String host : blk.getHosts() )
                hosts.add(host);
        }
        
        // GOOD, NOW ASSIGN HOST
        ArrayList<String> hostList = new ArrayList<String>(hosts);

        // just blindly split the given chunk into 'n' pieces
        final int numSplits = n == 0 ? conf.getInt(DEFAULT_NUM_MAP_SPLITS,
                conf.getInt(DEFAULT_NUM_SPLITS, 2)) : n;
        // long minSize =
        // conf.getInt(DEFAULT_MIN_MAP_SPLIT_SIZE,conf.getInt(DEFAULT_MIN_MAP_SPLIT_SIZE,4))
        // * MEGABYTES;

        // figure out appropriate size
        long chunkSize = remain / numSplits;
        final int nhosts = hostList.size();
        List<org.apache.hadoop.mapreduce.InputSplit> newSplits = new ArrayList<org.apache.hadoop.mapreduce.InputSplit>(numSplits);
        for (int i = 0; i < numSplits; ++i) {
            long sz = Math.min(chunkSize, remain);
            // FIXME shuffle the hosts to leverage multiple available hosts
            Collections.rotate(hostList, 1);
            org.apache.hadoop.mapreduce.lib.input.FileSplit fsplit = new org.apache.hadoop.mapreduce.lib.input.FileSplit(fileName, startOffset, sz, hostList.toArray(new String[nhosts]));
//            org.apache.hadoop.mapreduce.lib.input.FileSplit fsplit = new org.apache.hadoop.mapreduce.lib.input.FileSplit(fileName, startOffset, sz, hostList);

            newSplits.add(fsplit);
            startOffset += sz;
            remain -= sz;
//            if ( LOG.isDebugEnabled() ) {
//                LOG.debug(fsplit);
//                LOG.debug(Arrays.toString(fsplit.getLocations()));
//            }
        }
      
        return newSplits;
    }

    private static List<org.apache.hadoop.mapreduce.InputSplit> split(
            Configuration job,
            org.apache.hadoop.mapreduce.lib.input.CombineFileSplit s, int n)
            throws IOException {
        // FIXME check split size.
        // FIXME one strategy is having one mapper per file.
        throw new UnsupportedOperationException();
    }
    
    // for old splits
    
    public static List<org.apache.hadoop.mapred.InputSplit> split(
            Configuration job, org.apache.hadoop.mapred.InputSplit s)
            throws IOException {
        return split(job, s, DEFAULT_SPLITS);
    }

    public static List<org.apache.hadoop.mapred.InputSplit> split(
            Configuration job, org.apache.hadoop.mapred.InputSplit s, int n)
            throws IOException {
        if (s instanceof org.apache.hadoop.mapred.FileSplit) {
            return split(job, (org.apache.hadoop.mapred.FileSplit) s, n);
        } else if (s instanceof org.apache.hadoop.mapred.lib.CombineFileSplit) {
            return split(job, (org.apache.hadoop.mapred.lib.CombineFileSplit) s, n);
        }
        throw new IOException("Unsupported input split type: " + s.getClass());
    }

    private static List<org.apache.hadoop.mapred.InputSplit> split(
            Configuration conf, org.apache.hadoop.mapred.FileSplit s, int n) throws IOException {
        if (LOG.isInfoEnabled())
            LOG.info("Split filesplit (" + s + ") into " + n);

        Path fileName = s.getPath();
        long startOffset = s.getStart();
        long remain = s.getLength();
        
        // EXTRACT HOST INFORMATION
        HashSet<String> hosts = new HashSet<String>();
        FileSystem fs = fileName.getFileSystem(conf);
//        FileStatus fileStatus = fs.getFileStatus(fileName);
//        BlockLocation[] blocs = fs.getFileBlockLocations(fileStatus, startOffset, remain);
        BlockLocation[] blocs = fs.getFileBlockLocations(fileName, startOffset, remain);
        for ( BlockLocation blk : blocs ) {
            for ( String host : blk.getHosts() )
                hosts.add(host);
        }
        
        // GOOD, NOW ASSIGN HOST
        ArrayList<String> hostList = new ArrayList<String>(hosts);
        
        // just blindly split the given chunk into 'n' pieces
        final int numSplits = n == 0 ? conf.getInt(DEFAULT_NUM_MAP_SPLITS,conf.getInt(DEFAULT_NUM_SPLITS, 2)) : n;
        
        // long minSize =
        // conf.getInt(DEFAULT_MIN_MAP_SPLIT_SIZE,conf.getInt(DEFAULT_MIN_MAP_SPLIT_SIZE,4))
        // * MEGABYTES;

        // figure out appropriate size
        long chunkSize = remain / numSplits;
        final int nhosts = hostList.size();
        List<org.apache.hadoop.mapred.InputSplit> newSplits = new ArrayList<org.apache.hadoop.mapred.InputSplit>(numSplits);
        for (int i = 0; i < numSplits; ++i) {
            long sz = Math.min(chunkSize, remain);
            Collections.rotate(hostList, 1);
            newSplits.add(new org.apache.hadoop.mapred.FileSplit(fileName, startOffset, sz, hostList.toArray(new String[nhosts])));
//            newSplits.add(new org.apache.hadoop.mapred.FileSplit(fileName, startOffset, sz, hostList));
            startOffset += sz;
            remain -= sz;
        }
        
        if ( LOG.isDebugEnabled() ) {
            LOG.debug("new inputsplits = "+newSplits);
        }

        return newSplits;
    }

    private static List<org.apache.hadoop.mapred.InputSplit> split(
            Configuration job,
            org.apache.hadoop.mapred.lib.CombineFileSplit s, int n)
            throws IOException {
        // FIXME check split size.
        // FIXME one strategy is having one mapper per file.
        throw new UnsupportedOperationException();
    }
    
    private static List<?> split(Configuration conf,
            org.apache.hadoop.mapred.FileSplit s, List<Partition> parts,List<Range> range) throws IOException {
        if (LOG.isInfoEnabled())
            LOG.info("Split filesplit (" + s + ") into " + range.size());

        Path fileName = s.getPath();
        long startOffset = s.getStart();
        long remain = s.getLength();
        
        // EXTRACT HOST INFORMATION
        HashSet<String> hosts = new HashSet<String>();
        FileSystem fs = fileName.getFileSystem(conf);
//        FileStatus fileStatus = fs.getFileStatus(fileName);
//        BlockLocation[] blocs = fs.getFileBlockLocations(fileStatus, startOffset, remain);
        BlockLocation[] blocs = fs.getFileBlockLocations(fileName, startOffset, remain);
        for ( BlockLocation blk : blocs ) {
            for ( String host : blk.getHosts() )
                hosts.add(host);
        }
        
        // GOOD, NOW ASSIGN HOST
        ArrayList<String> hostList = new ArrayList<String>(hosts);
        
        boolean useNewApi = conf.getBoolean("mapred.mapper.new-api", false);

        // just blindly split the given chunk into 'n' pieces.
        final int numSplits = range.size();
        final int nhosts = hostList.size();
        List newSplits = new ArrayList(numSplits);
         for (int i = 0; i < numSplits; ++i) {
             Range r = range.get(i);
             MapInputPartition begin = (MapInputPartition)(parts.get(r.begin())); // for offset
             Collections.rotate(hostList, 1);
             
             if ( useNewApi ) {
                 newSplits.add(new org.apache.hadoop.mapreduce.lib.input.FileSplit(fileName, begin.getOffset(), r.getTotalBytes(), hostList.toArray(new String[nhosts])));
             } else {
                 newSplits.add(new org.apache.hadoop.mapred.FileSplit(fileName, begin.getOffset(), r.getTotalBytes(), hostList.toArray(new String[nhosts])));
             }
//            newSplits.add(new org.apache.hadoop.mapred.FileSplit(fileName, startOffset, sz, hostList));
        }
        
        if ( LOG.isDebugEnabled() ) {
            LOG.debug("new inputsplits = "+newSplits);
        }

        return newSplits;
    }
}
