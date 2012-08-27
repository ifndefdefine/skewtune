package skewtune.mapreduce;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.PriorityQueue;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskType;
import skewtune.mapreduce.JobInProgress.ReactionContext;
import skewtune.mapreduce.PartitionMapInput.MapInputPartition;
import skewtune.mapreduce.PartitionMapOutput.MapOutputPartition;
import skewtune.mapreduce.PartitionPlanner.ClusterInfo;
import skewtune.mapreduce.PartitionPlanner.OptEstimate;
import skewtune.mapreduce.PartitionPlanner.Partition;
import skewtune.mapreduce.PartitionPlanner.Plan;
import skewtune.mapreduce.PartitionPlanner.PlanSpec;
import skewtune.mapreduce.PartitionPlanner.PlanType;
import skewtune.mapreduce.PartitionPlanner.Range;
import skewtune.mapreduce.SimpleCostModel.Estimator;
import skewtune.mapreduce.SimpleCostModel.IncrementalEstimator;
import skewtune.mapreduce.util.PrimeTable;
import skewtune.utils.Utils;

public class PartitionPlanner implements SkewTuneJobConfig {
    public static final Log LOG = LogFactory.getLog(PartitionPlanner.class);
    
    /**
     * constants related to cost of reactive job. this is lame but no choice... :-(
     */
    public static final double STARTUP_OVERHEAD = 10.; // to run a job, we expect 10 sec delay
    public static final double SCAN_OVERHEAD = 60.;    // we expect scan/shuffle takes a minute
    
    public static enum PlanType {
        REHASH,
        FIRST,
        LONGEST,
        MAXSLOTS,
        LONGEST2,
        LONGEST_MODEL, 
        LINEAR
    }
    
    public static interface PlanSpec {
        public PlanType getType();
        public boolean requireScan();
        public boolean requireClusterInfo();
        public boolean requireCosts();
        public Plan plan(ReactionContext context,ClusterInfo clusterInfo,List<Partition> part,OptEstimate est) throws IOException;
    }
    
    public static class ClusterInfo {
        final TaskType slotType;
        /**
         * number of slots of slotType in the cluster
         */
        final int maxSlots;
        /**
         * total running slots
         */
        final int runningSlots;
        /**
         * total running slots that running skewtune jobs
         */
        final int runningSkewTuneSlots;
        /**
         * slot limit -- number of slots to run speculative/handover jobs
         */
        int limit;
        
        // allocation list
        final double[] availList;
        final int endIndex;
        
        public ClusterInfo(TaskType type,ClusterMetrics metrics) {
            slotType = type;
            maxSlots = type == TaskType.MAP ? metrics.getMapSlotCapacity() : metrics.getReduceSlotCapacity();
            runningSlots = 0;
            runningSkewTuneSlots = 0;
            limit = maxSlots;
            availList = new double[0];
            endIndex = 0;
        }
       
        public ClusterInfo(TaskType type, int maxSlots, int runningSlots,
                int runningSkewTuneSlots, double[] remainingTimes, int to) {
            slotType = type;
            this.maxSlots = maxSlots;
            this.runningSlots = runningSlots;
            this.runningSkewTuneSlots = runningSkewTuneSlots;
            this.availList = remainingTimes;
            this.endIndex = to;
        }
    }
    
    public static interface Partition extends Writable {
        public int  getIndex();
        public void setIndex(int i);
        public long getLength();
        
        public double getCost();
        public void setCost(double c);
    }

    public static abstract class Plan {
        protected long plannedAt;
        
        void setPlannedAt(long now) { plannedAt = now; }
        
        /**
         * number of new tasks
         * @return
         */
        public abstract int getNumPartitions();

        /**
         * set schedule order if we reorder subpartition in order of longest processing time
         * @return the mapping between key order to scheduling order. the index of array
         * refers to the partition in key order, the value of array refers to the partition
         * in schedule order (i.e., the task id of reactive job). <code>null</code> if the
         * plan does not support reorder scheduling.
         */
        public int[] getScheduleOrder() {
        	return null;
        }
        
//        protected abstract float getTimePerByte();
        protected abstract double[] getExpectedTimes();
        protected abstract double[] getExpectedTimes(ClusterInfo cluster,List<Range> ranges);
        
//        protected abstract void fillExpectedTime(Configuration conf,ClsuterInfo cluster,double[] time,int nMaps,int nReduces);
        
        public PlannedJob getPlannedJob(JobInProgress jip) {
            return new PlannedJob(jip,plannedAt,getExpectedTimes());
        }
    }
    
    public static abstract class ReactiveMapPlan extends Plan {
        public abstract List<?> getSplits() throws IOException;
    
        @Override
        protected double[] getExpectedTimes(ClusterInfo cluster,List<Range> ranges) {
            double[] time = new double[ranges.size()];
            int i = 0;
            int limit = Math.min(time.length, cluster.availList.length);
            
            for ( ; i < limit; ++i ) {
                time[i] = Math.max(STARTUP_OVERHEAD, cluster.availList[i]) + ranges.get(i).getTotalTime();
            }
            
            if ( i < time.length ) {
                limit = time.length - i;
                for ( int j = 0; j < limit; ++j, ++i ) {
                    time[i] = time[j] + ranges.get(i).getTotalTime();
                }
            }
            
            return time;
        }
        
        protected List<?> getSplits(Configuration conf,FileSplit s,List<Partition> parts,List<Range> range) throws IOException {
            if (LOG.isInfoEnabled())
                LOG.info("Split filesplit (" + s + ") into " + range.size());

            Path fileName = s.getPath();
            long startOffset = s.getStart();
            long remain = s.getLength();
            
            // EXTRACT HOST INFORMATION
            HashSet<String> hosts = new HashSet<String>();
            FileSystem fs = fileName.getFileSystem(conf);
//            FileStatus fileStatus = fs.getFileStatus(fileName);
//            BlockLocation[] blocs = fs.getFileBlockLocations(fileStatus, startOffset, remain);
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
//                newSplits.add(new org.apache.hadoop.mapred.FileSplit(fileName, startOffset, sz, hostList));
            }
            
            if ( LOG.isTraceEnabled() ) {
                LOG.trace("new inputsplits = "+newSplits);
            }

            return newSplits;
        }
    }
    
    public static abstract class ReactiveReducePlan extends Plan {
        public abstract void write(DataOutput out) throws IOException;
        
        /**
         * partitioner class name
         * @return
         */
        public abstract String getPartitionClassName(boolean newApi);
        
        @Override
        protected double[] getExpectedTimes(ClusterInfo cluster,List<Range> ranges) {
            double[] time = new double[ranges.size()];
            int i = 0;
            int limit = Math.min(time.length, cluster.availList.length);
            
            for ( ; i < limit; ++i ) {
                time[i] = Math.max(STARTUP_OVERHEAD + SCAN_OVERHEAD, cluster.availList[i]) + ranges.get(i).getTotalTime();
            }
            
            if ( i < time.length ) {
                limit = time.length - i;
                for ( int j = 0; j < limit; ++j, ++i ) {
                    time[i] = time[j] + ranges.get(i).getTotalTime();
                }
            }

            return time;
        }
    }
    
    static class OptEstimate {
        final double optTime;
        final int slots;
        
        OptEstimate(double t,int s) {
            this.optTime = t;
            this.slots = s;
        }
        
        public double getTime() { return optTime; }
        public int getSlots() { return slots; }
        
        @Override
        public String toString() {
            return String.format("%.3f secs using %d slots",optTime,slots);
        }
    }
    
    static class Range {
        final int from;
        final int end;
        final long totalBytes;
        final double totalTime;
        
        double scheduleAt;
        
        transient int index;

        public Range(int from, int end, long bytes, double t) {
            this.from = from;
            this.end = end;
            this.totalBytes = bytes;
            this.totalTime = t;
        }
        
        public List<Partition> subList(List<Partition> l) {
            return l.subList(from, end);
        }
        
        public int begin() {
            return from;
        }
        public int end() {
            return end;
        }
        
        /**
         * @return number of partition items in this range
         */
        public int getLength() {
            return end - from;
        }
        
        public long getTotalBytes() {
            return totalBytes;
        }
        public double getTotalTime() {
            return totalTime;
        }
        
        public double getScheduleAt() {
            return scheduleAt;
        }
        public void setScheduleAt(double t) {
            scheduleAt = t;
        }
        
        public int getIndex() { return index; }
        public void setIndex(int i) { index = i; }
        
        @Override
        public String toString() {
            return String.format("[%d,%d) %d bytes (%.3f secs) => %d",from,end,totalBytes,totalTime,index);
        }
    }

    
    public static OptEstimate getEstimatedOptimalTime(double[] availList,float totalTime) {
        double remain = totalTime;
        double finTime = 0.;

        int nSlots = 0;
        
        while ( nSlots < availList.length && remain > 0. ) {
            // compute new slots
            double availAt = availList[nSlots];
            int endSlot = nSlots+1;
            for ( ; endSlot < availList.length && availAt == availList[endSlot]; ++endSlot );
            
            // endSlot points to the slot that has next availalb slot
            
            int numNewSlots = endSlot - nSlots;

            if ( endSlot < availList.length ) {
                double slotWidth = availList[endSlot] - availAt;
                double workForPrevSlots = slotWidth * nSlots;
                
                if ( workForPrevSlots > remain ) {
                    // we can complete the task without launching a new task
                    finTime += ( remain / nSlots );
                    remain = 0.;
                } else {
                    double workForAllSlots = slotWidth * (nSlots+numNewSlots);
    
                    if ( workForAllSlots > remain ) {
                        // we can complete the task with all new tasks
                        finTime += ( remain / (nSlots + numNewSlots) );
                        remain = 0.;
                    } else {
                        // we will use all the new slots
                        remain -= workForAllSlots;
                        finTime += slotWidth;
                    }
                    nSlots += numNewSlots;
                }
            } else {
                // we have consumed every slots. just spread it.
                nSlots += numNewSlots;
            }
        }
        
        if ( remain > 0. ) {
            // we still have some works to do
            finTime += (remain / nSlots );
        }
        
        return new OptEstimate(finTime,nSlots);
    }
    
    public static Range segment(List<? extends Partition> l,int from,double targetTime, double maxTime,float timePerByte) {
        return segment(l,from,targetTime,maxTime,timePerByte,false);
    }

    
    public static Range segment(List<? extends Partition> l,int from,double targetTime, double maxTime,float timePerByte,boolean bothDir) {
        long targetBytes = (long)Math.ceil(targetTime / timePerByte);
        long remainingBytes = targetBytes;
        int end = from;
        long totalBytes = 0;

//        System.err.println("target time  = "+targetTime+"/max time = "+maxTime);
//        System.err.println("time per byte= "+timePerByte+"/target bytes = "+targetBytes);
        
        // we should assign at least one block
        Partition p = l.get(end);
        remainingBytes = targetBytes - p.getLength();
        totalBytes = p.getLength();
        ++end;
        
        while ( remainingBytes > 0 && end < l.size() ) {
            p = l.get(end);
            boolean includeThis = true;
            
            if ( remainingBytes < p.getLength() ) {
                // before
                double prevTime = totalBytes * timePerByte;
                double newTime = (totalBytes + p.getLength()) * timePerByte;
                
                if ( newTime < maxTime ) {
                    // we are good. include p in this partition
                } else {
                    // prevTime < targetTime
                    //            targetTime =< maxTime < newTime
                    if ( (newTime - maxTime) < (maxTime - prevTime) ) {
                        // better to increase maxTime. include this partition
                    } else {
                        // don't increase maxTime.
                        // do not include this partition
                        includeThis = false;
                    }
                }
            }
            
            if ( includeThis ) {
                remainingBytes -= p.getLength();
                totalBytes += p.getLength();
                ++end;
            } else {
                break;
            }
        }
        
//        System.err.printf("from=%d,end=%d,remain=%d\n",from,end,remainingBytes);
        
        // we got some remaining bytes and reach to the end of the list.
        // now it's time to probe the other direction
        if ( bothDir && (remainingBytes > 0 && end == l.size()) ) {
            long prevRemainingBytes = remainingBytes;
            while ( remainingBytes > 0 && from >= 0 ) {
                p = l.get(from);
                boolean includeThis = true;
                
                if ( remainingBytes < p.getLength() ) {
                    // before
                    double prevTime = totalBytes * timePerByte;
                    double newTime = (totalBytes + p.getLength()) * timePerByte;
                    
                    if ( newTime < maxTime ) {
                        // we are good. include p in this partition
                    } else {
                        // prevTime < targetTime
                        //            targetTime =< maxTime < newTime
                        if ( (newTime - maxTime) < (maxTime - prevTime) ) {
                            // better to increase maxTime. include this partition
                        } else {
                            // don't increase maxTime.
                            // do not include this partition
                            includeThis = false;
                        }
                    }
                }
                
                if ( includeThis ) {
                    remainingBytes -= p.getLength();
                    totalBytes += p.getLength();
                    --from;
                } else {
                    break;
                }
            }
            if ( prevRemainingBytes > remainingBytes ) {
                // we have included extra data
                ++from;
            }
        }
        
        // good. we have all information now
        int realEnd = end < l.size() ? l.get(end).getIndex() : l.get(l.size()-1).getIndex()+1;
        
        return new Range(l.get(from).getIndex(),realEnd,totalBytes,totalBytes * timePerByte);
    }
        

    public static Range segmentByCost(List<? extends Partition> l,int from,double targetTime, double maxTime,IncrementalEstimator e,boolean bothDir) {
        int end = from;
        double remainingCosts = targetTime;
        double totalCosts = 0.;
        long totalBytes = 0;
        
        e.reset();

//        System.err.println("target time  = "+targetTime+"/max time = "+maxTime);
//        System.err.println("time per byte= "+timePerByte+"/target bytes = "+targetBytes);
        
        // we should assign at least one block
        Partition p = l.get(end);
        e.add(p);
        totalBytes = p.getLength();
        ++end;
        
        while ( e.getCost() < targetTime && end < l.size() ) {
            p = l.get(end);
            e.add(p);
            totalBytes += p.getLength();
            ++end;
        }
        
        while ( e.getCost() < maxTime && end < l.size() ) {
            p = l.get(end);
            if ( e.cost(p) < maxTime ) {
                e.add(p);
                totalBytes += p.getLength();
            } else {
                break;
            }
            ++end;
        }
        
        double myCost = e.getCost();
        
//        System.err.printf("from=%d,end=%d,remain=%d\n",from,end,remainingBytes);
        
        // we got some remaining bytes and reach to the end of the list.
        // now it's time to probe the other direction
        if ( bothDir && (e.getCost() < targetTime && end == l.size()) ) {
            while ( e.getCost() < targetTime && from >= 0 ) {
                p = l.get(from);
                e.add(p);
                totalBytes += p.getLength();
                --from;
            }
        }
        if ( bothDir && (e.getCost() < maxTime && end == l.size() && from >= 0 ) ) {
            while ( e.getCost() < maxTime && from >= 0 ) {
                p = l.get(from);
                if ( e.cost(p) < maxTime ) {
                    e.add(p);
                    totalBytes += p.getLength();
                } else {
                    break;
                }
                --from;
            }
        }
        
        // we got one smaller from counter
        if ( myCost != e.getCost() ) {
            ++from;
        }
        
        
        // good. we have all information now
        int realEnd = end < l.size() ? l.get(end).getIndex() : l.get(l.size()-1).getIndex()+1;
        
        return new Range(l.get(from).getIndex(),realEnd,totalBytes,e.getCost());
    }


    static class PlanGreedyFirst implements PlanSpec {
		@Override
        public PlanType getType() {
            return PlanType.FIRST;
        }

        @Override
        public boolean requireScan() {
            return true;
        }
        
        @Override
        public boolean requireClusterInfo() {
            return true;
        }

        @Override
        public Plan plan(final ReactionContext context, final ClusterInfo clusterInfo, final List<Partition> part, OptEstimate est) throws IOException {
            double[] availList = clusterInfo.availList;
            double optTime = est.getTime();
            final float timePerByte = context.getTimePerByte();
            
            final List<Range> boundary = new ArrayList<Range>();
            double maxTime = optTime;
            int from = 0;
            
            for ( int i = 0; i < availList.length && from < part.size(); ++i ) {
                double targetTime = optTime - availList[i];
//                Range r = segment(part,from,targetTime,maxTime - availList[i],timePerByte);
                Range r = segment(part,from,maxTime - availList[i],maxTime - availList[i],timePerByte);
                double newTime = availList[i] + r.getTotalTime();
                if ( maxTime < newTime ) {
                    maxTime = newTime;
                }
                boundary.add(r);
                from = r.end();
            }
            
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("time per bytes = "+timePerByte);
                LOG.debug("new estimated time = "+maxTime);
                LOG.debug("partition boundaries = "+boundary.toString());
            }
            
            final int nSlots = boundary.size();
            
            if ( context.getTaskType() == TaskType.MAP ) {
                final FileSplit split = context.getContext();
//                final List<?> splits = PartitionPlanner.getSplits(context.getConfiguration(), split, part, boundary);

                return new ReactiveMapPlan() {
                    @Override
                    public int getNumPartitions() {
                        return nSlots;
                    }

                    @Override
                    public List<?> getSplits() throws IOException {
                        return getSplits(context.getConfiguration(), split, part, boundary);
                    }
                   
                    @Override
                    protected double[] getExpectedTimes() {
                        return getExpectedTimes(clusterInfo, boundary);
                    }
                };
            } else {
                final BitSet excludes = new BitSet(boundary.size()-1);
                final List<byte[]> result = new ArrayList<byte[]>( boundary.size()-1 );
                for ( int i = 1; i < boundary.size(); ++i ) {
                    Range r = boundary.get(i);
                    MapOutputPartition partition = ((MapOutputPartition)part.get(r.begin()));
                    if ( partition.isMinKeyExcluded() ) {
                        excludes.set(result.size());
                    }
                    byte[] key = partition.getMinKey();
                    result.add(key);
                }

                return new ReactiveReducePlan() {
                    @Override
                    public int getNumPartitions() {
                        return nSlots;
                    }
    
                    @Override
                    public void write(DataOutput out) throws IOException {
                        for ( byte[] key : result ) {
                            WritableUtils.writeVInt(out, key.length);
                            out.write(key);
                        }
                        for ( int i = 0; i < result.size()+1; ++i ) {
                            WritableUtils.writeVInt(out, i);
                        }
                        int nexcludes = excludes.cardinality();
                        WritableUtils.writeVInt(out, nexcludes);
                        if ( nexcludes > 0 ) {
                            for (int i = excludes.nextSetBit(0); i >= 0; i = excludes.nextSetBit(i+1)) {
                                WritableUtils.writeVInt(out, i);
                            }
                        }
                    }
    
                    @Override
                    public String getPartitionClassName(boolean newApi) {
                        return skewtune.mapreduce.lib.partition.RangePartitioner.class.getName();
                    }
                    
                    @Override
                    public String toString() {
                        StringBuilder buf = new StringBuilder();
                        buf.append("Boundary:\n");
                        for ( Range r : boundary ) {
                            buf.append(r).append('\n');
                        }
                        buf.append("Keys:\n");
                        int i = 0;
                        for ( byte[] key : result ) {
                            buf.append(Utils.toHex(key));
                            if ( excludes.get(i) ) {
                                buf.append(" excluded");
                            }
                            buf.append('\n');
                            ++i;
                        }
                        return buf.toString();
                    }

                    @Override
                    protected double[] getExpectedTimes() {
                        return getExpectedTimes(clusterInfo, boundary);
                    }
                };
            }
        }

        @Override
        public boolean requireCosts() {
            // TODO Auto-generated method stub
            return false;
        }
    }

    static class PlanGreedyLinear implements PlanSpec {
		@Override
        public PlanType getType() {
            return PlanType.LINEAR;
        }

        @Override
        public boolean requireScan() {
            return true;
        }
        
        @Override
        public boolean requireClusterInfo() {
            return true;
        }

        @Override
        public Plan plan(final ReactionContext context, final ClusterInfo clusterInfo, final List<Partition> part, OptEstimate est) throws IOException {
            double[] availList = clusterInfo.availList;
            double optTime = est.getTime();
            final float timePerByte = context.getTimePerByte();
            
            final List<Range> boundary = new ArrayList<Range>();
            double maxTime = optTime;
            int from = 0;
            
            for ( int i = 0; i < availList.length && from < part.size(); ++i ) {
                double targetTime = optTime - availList[i];
//                Range r = segment(part,from,targetTime,maxTime - availList[i],timePerByte);
                Range r = segment(part,from,maxTime - availList[i],maxTime - availList[i],timePerByte);
                double newTime = availList[i] + r.getTotalTime();
                if ( maxTime < newTime ) {
                    maxTime = newTime;
                }
                r.setIndex(boundary.size());
                boundary.add(r);
                from = r.end();
            }
            
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("time per bytes = "+timePerByte);
                LOG.debug("new estimated time = "+maxTime);
                LOG.debug("partition boundaries = "+boundary.toString());
            }
            
            final int nSlots = boundary.size();
            
            Range[] ranges = boundary.toArray(new Range[nSlots]);
            Arrays.sort(ranges,new Comparator<Range>() {
				@Override
				public int compare(Range o1, Range o2) {
					return Double.compare(o2.getTotalTime(), o1.getTotalTime());
				}});
            
            final int[] scheduleOrder = new int[nSlots];
            for ( int i = 0; i < nSlots; ++i ) {
            	scheduleOrder[i] = ranges[i].getIndex();
            }
            
            if ( context.getTaskType() == TaskType.MAP ) {
                final FileSplit split = context.getContext();
//                final List<?> splits = PartitionPlanner.getSplits(context.getConfiguration(), split, part, boundary);

                return new ReactiveMapPlan() {
                    @Override
                    public int getNumPartitions() {
                        return nSlots;
                    }

                    @Override
                    public List<?> getSplits() throws IOException {
                        return getSplits(context.getConfiguration(), split, part, boundary);
                    }
                   
                    @Override
                    protected double[] getExpectedTimes() {
                        return getExpectedTimes(clusterInfo, boundary);
                    }
                    
                    @Override
                    public int[] getScheduleOrder() {
                    	return scheduleOrder;
                    }
                };
            } else {
                final BitSet excludes = new BitSet(boundary.size()-1);
                final List<byte[]> result = new ArrayList<byte[]>( boundary.size()-1 );
                for ( int i = 1; i < boundary.size(); ++i ) {
                    Range r = boundary.get(i);
                    MapOutputPartition partition = ((MapOutputPartition)part.get(r.begin()));
                    if ( partition.isMinKeyExcluded() ) {
                        excludes.set(result.size());
                    }
                    byte[] key = partition.getMinKey();
                    result.add(key);
                }

                return new ReactiveReducePlan() {
                    @Override
                    public int getNumPartitions() {
                        return nSlots;
                    }
    
                    @Override
                    public void write(DataOutput out) throws IOException {
                        for ( byte[] key : result ) {
                            WritableUtils.writeVInt(out, key.length);
                            out.write(key);
                        }
                        for ( int i = 0; i < result.size()+1; ++i ) {
                            WritableUtils.writeVInt(out, i);
                        }
                        int nexcludes = excludes.cardinality();
                        WritableUtils.writeVInt(out, nexcludes);
                        if ( nexcludes > 0 ) {
                            for (int i = excludes.nextSetBit(0); i >= 0; i = excludes.nextSetBit(i+1)) {
                                WritableUtils.writeVInt(out, i);
                            }
                        }
                    }
    
                    @Override
                    public String getPartitionClassName(boolean newApi) {
                        return skewtune.mapreduce.lib.partition.RangePartitioner.class.getName();
                    }
                    
                    @Override
                    public String toString() {
                        StringBuilder buf = new StringBuilder();
                        buf.append("Boundary:\n");
                        for ( Range r : boundary ) {
                            buf.append(r).append('\n');
                        }
                        buf.append("Keys:\n");
                        int i = 0;
                        for ( byte[] key : result ) {
                            buf.append(Utils.toHex(key));
                            if ( excludes.get(i) ) {
                                buf.append(" excluded");
                            }
                            buf.append('\n');
                            ++i;
                        }
                        return buf.toString();
                    }

                    @Override
                    protected double[] getExpectedTimes() {
                        return getExpectedTimes(clusterInfo, boundary);
                    }
                    
                    @Override
                    public int[] getScheduleOrder() {
                    	return scheduleOrder;
                    }
                };
            }
        }

        @Override
        public boolean requireCosts() {
            // TODO Auto-generated method stub
            return false;
        }
    }
    
    static class _range {
        int from; // inclusive
        int end;  // exclusive
        List<Partition> list;

        public _range(List<Partition> list) {
            from = 0;
            end = list.size();
            this.list = list;
        }

        public _range(List<Partition> list,int f, int e) {
            from = f;
            end = e;
            this.list = list.subList(from, end);
        }
        
        public List<Partition> getList() { return list; }
        public int length() { return end - from; }
        public int begin() { return from; }
        public int end() { return end; }
        
        public int relative(int i) {
            return i - from;
        }
        
        public boolean contains(int i) {
            return from <= i && i < end;
        }
        
        public _range[] subtract(List<Partition> part, int f,int e) {
            if ( from == f && end == e ) {
                return null; // nothing remains
            } else if ( from == f ) {
                return new _range[] { new _range(part,e,end) };
            } else if ( end == e ) {
                return new _range[] { new _range(part,from,f) };
            } 
            
            return new _range[] { new _range(part,from,f), new _range(part,e,end) };
        }
        
        @Override
        public String toString() {
            return String.format("[%d,%d)",from,end);
        }
    }
    
    static class PlanGreedyLongest implements PlanSpec {
        @Override
        public PlanType getType() {
            return PlanType.LONGEST;
        }

        @Override
        public boolean requireScan() {
            return true;
        }
        
        @Override
        public boolean requireClusterInfo() {
            return true;
        }

        @Override
        public Plan plan(final ReactionContext context, final ClusterInfo clusterInfo, final List<Partition> part, OptEstimate est) throws IOException {
            double[] availList = clusterInfo.availList;
            double optTime = est.getTime();
            int slots = est.getSlots();
            final float timePerByte = context.getTimePerByte();
            final List<Range> boundary = new ArrayList<Range>();
            
            BitSet allocMap = new BitSet(part.size());
            allocMap.set(0, part.size());
            
            List<Partition> sorted = new ArrayList<Partition>(part);
            Collections.sort(sorted,new Comparator<Partition>() {
                @Override
                public int compare(Partition o1, Partition o2) {
                    int sig = Long.signum(o2.getLength() - o1.getLength());
                    return sig == 0 ? o1.getIndex() - o2.getIndex() : sig;
                }});
            
            LinkedList<_range> fragments = new LinkedList<_range>();
            fragments.add(new _range(part));
            
            double maxTime = optTime;
            int from = 0;
            
            for ( int i = 0; i < slots && from < sorted.size() && ! allocMap.isEmpty(); ++i ) {
                double targetTime = optTime - availList[i];
                int index = sorted.get(from).getIndex();
                while ( ! allocMap.get(index) && from < sorted.size() ) {
                    index = sorted.get(++from).getIndex();
                }
                
                // good. now we found the next starting point
                ListIterator<_range> li = fragments.listIterator();
                _range rx = null;
                while ( li.hasNext() ) {
                    rx = li.next();
                    if ( rx.contains(index) ) break;
                }
                
                if ( rx == null ) {
                    // should not happen!
                    throw new IllegalStateException();
                }
                
                // lookup the sublist of part that contains the starting point
                Range r = segment(rx.getList(), rx.relative(index), maxTime - availList[i], maxTime - availList[i], timePerByte, true);
                double newTime = availList[i] + r.getTotalTime();
                if ( maxTime < newTime ) {
                    maxTime = newTime;
                }
                boundary.add(r);
                allocMap.clear(r.begin(),r.end());
                
                // add sublist map
                _range[] xx = rx.subtract(part,r.begin(),r.end());
                li.remove(); // update current list
                if ( xx != null ) {
                    if ( xx.length > 1 ) li.add(xx[1]);
                    li.add(xx[0]);
                }
            }
            
            if ( ! fragments.isEmpty() ) {
                for ( _range r : fragments ) {
                    long sz = 0;
                    for ( Partition p : r.getList() ) {
                        sz += p.getLength();
                    }
                    boundary.add(new Range(r.begin(), r.end(), sz, sz*timePerByte));
                }
            }
            
            // sort by total time and generate mapping (from largest (0) to smallest (N))
            ArrayList<Range> sortedRanges = new ArrayList<Range>(boundary);
            Collections.sort(sortedRanges,new Comparator<Range>(){
                @Override
                public int compare(Range o1, Range o2) {
                    return (int)Math.signum(o2.getTotalTime() - o1.getTotalTime());
                }});
            
            // now set ordering
            for ( int i = 0; i < boundary.size(); ++i ) {
                sortedRanges.get(i).setIndex(i);
            }
            
            // now should sort in range
            Collections.sort(boundary, new Comparator<Range>() {
                @Override
                public int compare(Range o1, Range o2) {
                    int rc = o1.begin() - o2.begin();
                    return rc == 0 ? o1.getLength() - o2.getLength() : rc;
                }});
            
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("time per bytes = "+timePerByte);
                LOG.debug("new estimated time = "+maxTime);
                LOG.debug("partition boundaries = "+boundary.toString());
            }

            System.err.println(boundary.toString());
            
            final int nSlots = boundary.size();
            
            if ( context.getTaskType() == TaskType.MAP ) {
                final FileSplit split = context.getContext();
//                final List<?> splits = PartitionPlanner.getSplits(context.getConfiguration(), split, part, boundary);

                return new ReactiveMapPlan() {
                    @Override
                    public int getNumPartitions() {
                        return nSlots;
                    }

                    @Override
                    public List<?> getSplits() throws IOException {
                        return getSplits(context.getConfiguration(), split, part, boundary);
                    }
                   
                    @Override
                    protected double[] getExpectedTimes() {
                        return getExpectedTimes(clusterInfo, boundary);
                    }
                };
            } else {
                final BitSet excludes = new BitSet(boundary.size()-1);
                final List<byte[]> result = new ArrayList<byte[]>( boundary.size()-1 );
                for ( int i = 1; i < boundary.size(); ++i ) {
                    Range r = boundary.get(i);
                    MapOutputPartition partition = ((MapOutputPartition)part.get(r.begin()));
                    if ( partition.isMinKeyExcluded() ) {
                        excludes.set(result.size());
                    }
                    byte[] key = partition.getMinKey();
                    result.add(key);
                }

                return new ReactiveReducePlan() {
                    @Override
                    public int getNumPartitions() {
                        return nSlots;
                    }
    
                    @Override
                    public void write(DataOutput out) throws IOException {
                        for ( byte[] key : result ) {
                            WritableUtils.writeVInt(out, key.length);
                            out.write(key);
                        }
                        for ( Range r : boundary ) {
                            WritableUtils.writeVInt(out, r.getIndex());
                        }
                        int nexcludes = excludes.cardinality();
                        WritableUtils.writeVInt(out, nexcludes);
                        if ( nexcludes > 0 ) {
                            for (int i = excludes.nextSetBit(0); i >= 0; i = excludes.nextSetBit(i+1)) {
                                WritableUtils.writeVInt(out, i);
                            }
                        }
                    }
    
                    @Override
                    public String getPartitionClassName(boolean newApi) {
                        return skewtune.mapreduce.lib.partition.RangePartitioner.class.getName();
                    }
                    
                    @Override
                    public String toString() {
                        StringBuilder buf = new StringBuilder();
                        buf.append("Boundary:\n");
                        int i = 0;
                        for ( Range r : boundary ) {
                            buf.append(r).append("  ").append(clusterInfo.availList[i] + r.getTotalTime()).append('\n');
                        }
                        buf.append("Keys:\n");
                        for ( byte[] key : result ) {
                            buf.append(Utils.toHex(key));
                            if ( excludes.get(i) ) {
                                buf.append(" excluded");
                            }
                            buf.append('\n');
                            ++i;
                        }
                        return buf.toString();
                    }

                    @Override
                    protected double[] getExpectedTimes() {
                        return getExpectedTimes(clusterInfo, boundary);
                    }
                };
            }
        }

        @Override
        public boolean requireCosts() {
            // TODO Auto-generated method stub
            return false;
        }
    }
    
    private static Double[] EMPTY_DOUBLE_ARRAY = new Double[0];


    static class PlanGreedyLongest2 implements PlanSpec {
        @Override
        public PlanType getType() {
            return PlanType.LONGEST2;
        }

        @Override
        public boolean requireScan() {
            return true;
        }
        
        @Override
        public boolean requireClusterInfo() {
            return true;
        }

        @Override
        public Plan plan(final ReactionContext context, final ClusterInfo clusterInfo, final List<Partition> part, OptEstimate est) throws IOException {
            double[] availList = clusterInfo.availList.clone();
            double optTime = est.getTime();
            int slots = est.getSlots();
            
            final long cutoffBytes = context.getRemainBytes() / clusterInfo.maxSlots; // force split if a record is larger than cutoff
            final float timePerByte = context.getTimePerByte();
            final List<Range> boundary = new ArrayList<Range>();
            
            BitSet allocMap = new BitSet(part.size());
            allocMap.set(0, part.size());
            
            List<Partition> sorted = new ArrayList<Partition>(part);
            Collections.sort(sorted,new Comparator<Partition>() {
                @Override
                public int compare(Partition o1, Partition o2) {
                    int sig = Long.signum(o2.getLength() - o1.getLength());
                    return sig == 0 ? o1.getIndex() - o2.getIndex() : sig;
                }});
            
            LinkedList<_range> fragments = new LinkedList<_range>();
            fragments.add(new _range(part));
            
            PriorityQueue<Double> pq = new PriorityQueue<Double>(Arrays.asList(ArrayUtils.toObject(availList)));

            if ( sorted.get(0).getLength() >= cutoffBytes ) {
                // BUILD AVAILABILITY LIST
                long alloced = 0;
                for ( Partition p : sorted ) {
                    if ( p.getLength() < cutoffBytes ) {
                        break;
                    }
                    int index = p.getIndex();
                    Range r = new Range(index,index+1,p.getLength(),p.getLength()*timePerByte);
                    r.setIndex(boundary.size());
                    boundary.add(r);
                    allocMap.clear(index);
                    
                    alloced += r.getTotalBytes();
                    
                    double t = pq.poll();
                    r.setScheduleAt(t);
                    pq.offer(t + r.getTotalTime());
                    
                    ListIterator<_range> li = fragments.listIterator();
                    _range rx = null;
                    while ( li.hasNext() ) {
                        rx = li.next();
                        if ( rx.contains(index) ) break;
                    }
                    
                    if ( rx == null ) {
                        // should not happen!
                        throw new IllegalStateException();
                    }
                    
                    // add sublist map
                    _range[] xx = rx.subtract(part,index,index+1);
                    li.remove(); // update current list
                    if ( xx != null ) {
                        if ( xx.length > 1 ) li.add(xx[1]);
                        li.add(xx[0]);
                    }
                }
                
                availList = ArrayUtils.toPrimitive(pq.toArray(EMPTY_DOUBLE_ARRAY));
                Arrays.sort(availList);
                
                // now update optTime
                est = getEstimatedOptimalTime(availList,(context.getRemainBytes() - alloced) * timePerByte);
                optTime = est.getTime();
                slots = est.getSlots();
                
//                System.out.println("Large Partition");
//                System.out.println(boundary);
//                System.out.println("Availlist = "+Arrays.toString(availList));
//                System.out.println(optTime + " "+slots);
            }
            
            int from = boundary.size();
            double maxTime = optTime;
            for ( int i = 0;
                    i < slots &&
//            while (
                    ! fragments.isEmpty()
                    && from < sorted.size()
                    && ! allocMap.isEmpty()
                    ; ++i
                    ) {
                int index = sorted.get(from).getIndex();
                while ( ! allocMap.get(index) && from < sorted.size() ) {
                    index = sorted.get(++from).getIndex();
                }
                
                // good. now we found the next starting point
                ListIterator<_range> li = fragments.listIterator();
                _range rx = null;
                while ( li.hasNext() ) {
                    rx = li.next();
                    if ( rx.contains(index) ) break;
                }
                
                if ( rx == null ) {
                    // should not happen!
                    throw new IllegalStateException();
                }
                
                double t = pq.poll();
                // lookup the sublist of part that contains the starting point
                Range r = segment(rx.getList(), rx.relative(index), maxTime - t, maxTime - t, timePerByte, true);
                r.setIndex(boundary.size());
                r.setScheduleAt(t);
                boundary.add(r);
                
                pq.offer(t+r.getTotalTime());
                
                double newTime = t + r.getTotalTime();
//                System.out.println(r + " @ "+ t + " new time = " + newTime + " max time = "+maxTime);

                if ( maxTime < newTime ) {
                    maxTime = newTime;
                }

                allocMap.clear(r.begin(),r.end());
                
                // add sublist map
                _range[] xx = rx.subtract(part,r.begin(),r.end());
                li.remove(); // update current list
                if ( xx != null ) {
                    if ( xx.length > 1 ) li.add(xx[1]);
                    li.add(xx[0]);
                }
            }
            
//            System.out.println(boundary);
            
            if ( ! fragments.isEmpty() ) {
                int numBoundarySoFar = boundary.size();
                for ( _range r : fragments ) {
                    long sz = 0;
                    for ( Partition p : r.getList() ) {
                        sz += p.getLength();
                    }
                    Range residue = new Range(r.begin(), r.end(), sz, sz*timePerByte);
                    boundary.add(residue);
                }
                
                List<Range> residual = boundary.subList(numBoundarySoFar, boundary.size());
                Collections.sort(residual,new Comparator<Range>() {
                    @Override
                    public int compare(Range r1, Range r2) {
                        return Long.signum(r2.getTotalBytes() - r1.getTotalBytes());
                    }});
                for ( Range residue : residual ) {
                    residue.setIndex(numBoundarySoFar++);
                    double t = pq.poll();
                    residue.setScheduleAt(t);
                    pq.offer(t + residue.getTotalTime());
                }
//                System.err.println("Fragments = " + fragments);
//                throw new IllegalStateException();
            }
            
            // now should sort in range
            Collections.sort(boundary, new Comparator<Range>() {
                @Override
                public int compare(Range o1, Range o2) {
                    int rc = o1.begin() - o2.begin();
                    return rc == 0 ? o1.getLength() - o2.getLength() : rc;
                }});
            
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("time per bytes = "+timePerByte);
                LOG.debug("new estimated time = "+maxTime);
                LOG.debug("partition boundaries = "+boundary.toString());
            }

            System.err.println(boundary.toString());
            
            final int nSlots = boundary.size();
            
            if ( context.getTaskType() == TaskType.MAP ) {
                final FileSplit split = context.getContext();
//                final List<?> splits = PartitionPlanner.getSplits(context.getConfiguration(), split, part, boundary);

                return new ReactiveMapPlan() {
                    @Override
                    public int getNumPartitions() {
                        return nSlots;
                    }

                    @Override
                    public List<?> getSplits() throws IOException {
                        return getSplits(context.getConfiguration(), split, part, boundary);
                    }
                   
                    @Override
                    protected double[] getExpectedTimes() {
                        double[] time = new double[boundary.size()];
                        for ( int i = 0; i < time.length; ++i ) {
                            time[i] = Math.max(STARTUP_OVERHEAD+SCAN_OVERHEAD, boundary.get(i).getScheduleAt()) + boundary.get(i).getTotalTime();
                        }
                        return time;
                    }
//                        return getExpectedTimes(clusterInfo, boundary);
                };
            } else {
                final BitSet excludes = new BitSet(boundary.size()-1);
                final List<byte[]> result = new ArrayList<byte[]>( boundary.size()-1 );
                for ( int i = 1; i < boundary.size(); ++i ) {
                    Range r = boundary.get(i);
                    MapOutputPartition partition = ((MapOutputPartition)part.get(r.begin()));
                    if ( partition.isMinKeyExcluded() ) {
                        excludes.set(result.size());
                    }
                    byte[] key = partition.getMinKey();
                    result.add(key);
                }

                return new ReactiveReducePlan() {
                    @Override
                    public int getNumPartitions() {
                        return nSlots;
                    }
    
                    @Override
                    public void write(DataOutput out) throws IOException {
                        for ( byte[] key : result ) {
                            WritableUtils.writeVInt(out, key.length);
                            out.write(key);
                        }
                        for ( Range r : boundary ) {
                            WritableUtils.writeVInt(out, r.getIndex());
                        }
                        int nexcludes = excludes.cardinality();
                        WritableUtils.writeVInt(out, nexcludes);
                        if ( nexcludes > 0 ) {
                            for (int i = excludes.nextSetBit(0); i >= 0; i = excludes.nextSetBit(i+1)) {
                                WritableUtils.writeVInt(out, i);
                            }
                        }
                    }
    
                    @Override
                    public String getPartitionClassName(boolean newApi) {
                        return skewtune.mapreduce.lib.partition.RangePartitioner.class.getName();
                    }
                    
                    @Override
                    public String toString() {
                        StringBuilder buf = new StringBuilder();
                        buf.append("Boundary:\n");
                        int i = 0;
                        for ( Range r : boundary ) {
                            buf.append(r).append("  ").append(clusterInfo.availList[i] + r.getTotalTime()).append('\n');
                        }
                        buf.append("Keys:\n");
                        for ( byte[] key : result ) {
                            buf.append(Utils.toHex(key));
                            if ( excludes.get(i) ) {
                                buf.append(" excluded");
                            }
                            buf.append('\n');
                            ++i;
                        }
                        return buf.toString();
                    }

                    @Override
                    protected double[] getExpectedTimes() {
                        double[] time = new double[boundary.size()];
                        for ( int i = 0; i < time.length; ++i ) {
                            time[i] = Math.max(STARTUP_OVERHEAD, boundary.get(i).getScheduleAt()) + boundary.get(i).getTotalTime();
                        }
                        return time;
//                        return getExpectedTimes(clusterInfo, boundary);
                    }
                };
            }
        }

        @Override
        public boolean requireCosts() {
            // TODO Auto-generated method stub
            return false;
        }
    }
    
    

    
    static class PlanRehash implements PlanSpec {
        @Override
        public PlanType getType() {
            return PlanType.REHASH;
        }

        @Override
        public boolean requireScan() {
            return false;
        }
        
        @Override
        public boolean requireClusterInfo() {
            return true;
        }

        @Override
        public Plan plan(final ReactionContext context,final ClusterInfo clusterInfo,
                final List<Partition> part, OptEstimate est) throws IOException {
            double t = est.getTime();
            final int nSlots = est.getSlots();
            final float timePerByte = context.getTimePerByte();
            final long remainBytes = context.getRemainBytes();
            
            double[] ratio = new double[nSlots];
            double sum = 0.;
            for ( int i = 0; i < nSlots; ++i ) {
                ratio[i] = ( t - clusterInfo.availList[i]);
                sum += ratio[i];
            }
            
            int prime = PrimeTable.getNearestPrime((int)Math.ceil(sum)<<1);
//            int acc = 0;
            // [ 0 1 2 3 4 5 ]
            // [0 1 2 3 4 5 6]
            final int[] portions = new int[nSlots];
            
            // FIXME revise this loop to proportionally allocate
//            for ( int i = 0; i < nSlots-1; ++i ) {
//                int alloc = (int)Math.round(prime * (ratio[i] / sum));
//                acc += alloc;
//                portions[i] = Math.min(acc, prime);
//            }
            double acc = 0.;
            for ( int i = 0; i < nSlots-1; ++i ) {
                acc += ratio[i];
                int alloc = (int)Math.round( prime * acc / sum );
                portions[i] = Math.min(alloc, prime);
            }

            portions[nSlots-1] = prime;
            
            final List<Range> ranges = new ArrayList<Range>(nSlots);
            long accSz = 0;
            // now evenly distribute the range
            for ( int i = 0; i < nSlots; ++i ) {
                long sz = Math.min(remainBytes,(long)(remainBytes * (portions[i] / (double)prime) + 0.5));
                ranges.add(new Range(i, i+1, sz - accSz, (sz - accSz) * context.getTimePerByte()));
                accSz = sz;
            }
            
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("nslots = "+nSlots+"/sum = "+sum+"/chosen prime number = "+prime);
                LOG.debug("ratio = "+Arrays.toString(ratio));
                LOG.debug("rehash partition plan = "+Arrays.toString(portions));
                LOG.debug("rehash ranges = "+ranges);
            }

            if ( context.getTaskType() == TaskType.MAP ) {
                final FileSplit split = context.getContext();
                
                long startOffset = split.getStart();
                long remain = remainBytes;
                
                // now evenly distribute the range
                for ( Range r : ranges ) {
                    long sz = r.getTotalBytes();
                    part.add(new PartitionMapInput.MapInputPartition(startOffset,sz));
                    startOffset += sz;
                }
                
                return new ReactiveMapPlan() {
                    @Override
                    public int getNumPartitions() {
                        return nSlots;
                    }

                    @Override
                    public List<?> getSplits() throws IOException {
                        return getSplits(context.getConfiguration(),split,part,ranges);
                    }

                    @Override
                    protected double[] getExpectedTimes() {
                        return getExpectedTimes(clusterInfo, ranges);
                    }
                };
            } else {                
                return new ReactiveReducePlan() {
                    @Override
                    public int getNumPartitions() {
                        return nSlots;
                    }
    
                    @Override
                    public void write(DataOutput out) throws IOException {
                        for ( int v : portions ) {
                            WritableUtils.writeVInt(out, v);
                        }
                        for ( int i = 0; i < nSlots; ++i ) {
                            WritableUtils.writeVInt(out, i);
                        }
                    }
    
                    @Override
                    public String getPartitionClassName(boolean newApi) {
                        return newApi ? skewtune.mapreduce.lib.partition.RehashDelegatePartitioner.class.getName()
                                : skewtune.mapred.RehashDelegatePartitioner.class.getName();
                    }
                    
                    @Override
                    public String toString() {
                        return Arrays.toString(portions);
                    }
                    
                    @Override
                    protected double[] getExpectedTimes() {
                        return getExpectedTimes(clusterInfo, ranges);
                    }
                };
            }
        }

        @Override
        public boolean requireCosts() {
            return false;
        }
    }
    
    static class PlanMaxSlots implements PlanSpec {
        @Override
        public PlanType getType() {
            return PlanType.MAXSLOTS;
        }

        @Override
        public boolean requireScan() {
            return false;
        }
        
        @Override
        public boolean requireClusterInfo() {
            return true;
        }

        @Override
        public Plan plan(final ReactionContext context, final ClusterInfo clusterInfo, final List<Partition> part, OptEstimate est) throws IOException {
            final int maxSlots = clusterInfo.maxSlots;
            final float timePerByte = context.getTimePerByte();
            final long remainBytes = context.getRemainBytes();
            final List<Range> ranges = new ArrayList<Range>(maxSlots);
            

            long remain = remainBytes;
            long chunkSize = (long)((remain / (double)maxSlots + 0.5));
            for ( int i = 0; i < maxSlots; ++i ) {
                long sz = Math.min(chunkSize,remain);
                ranges.add(new Range(i, i+1, sz, sz * context.getTimePerByte()));
                remain -= sz;
            }
            
            if ( context.getTaskType() == TaskType.MAP ) {
                final FileSplit split = context.getContext();
                
                long startOffset = split.getStart();
                // now evenly distribute the range
                for ( Range r : ranges ) {
                    long sz = r.getTotalBytes();
                    part.add(new PartitionMapInput.MapInputPartition(startOffset,sz));
                    startOffset += sz;
                }
                
                return new ReactiveMapPlan() {
                    @Override
                    public int getNumPartitions() {
                        return maxSlots;
                    }
                    
                    @Override
                    public List<?> getSplits() throws IOException {
                        return this.getSplits(context.getConfiguration(), split, part, ranges);
                    }

                    @Override
                    protected double[] getExpectedTimes() {
                        return getExpectedTimes(clusterInfo, ranges);
                    }
                };
            } else {
                final int prime = PrimeTable.getNearestPrime(Math.max(context.getNumReduceTasks(), maxSlots));
                
                return new ReactiveReducePlan() {
                    @Override
                    public int getNumPartitions() {
                        return maxSlots;
                    }
    
                    @Override
                    public void write(DataOutput out) throws IOException {
                        WritableUtils.writeVInt(out, prime);
                    }
    
                    @Override
                    public String getPartitionClassName(boolean newApi) {
                        return newApi ? skewtune.mapreduce.lib.partition.DoubleHashDelegatePartitioner.class.getName() :
                            skewtune.mapred.DoubleHashDelegatePartitioner.class.getName();
                    }

                    @Override
                    protected double[] getExpectedTimes() {
                        return getExpectedTimes(clusterInfo, ranges);
                    }
                };
            }
        }

        @Override
        public boolean requireCosts() {
            return false;
        }
    }

    public static PlanSpec getPlanSpec(TaskInProgress tip) {
        return getPlanSpec(tip.getJob());
    }

    public static PlanSpec getPlanSpec(JobInProgress jip) {
        return getPlanSpec(jip.getConfiguration());
    }
    
    public static PlanSpec getPlanSpec(Configuration conf) {
        return specs.get(conf.getEnum(SKEWTUNE_REPARTITION_STRATEGY, PlanType.LINEAR));
    }
    
    public static PlanSpec getPlanSpec(PlanType type) {
        return specs.get(type);
    }

    public static Plan plan(ReactionContext action,ClusterInfo clusterInfo,long now) throws IOException {
        return plan(action,clusterInfo,new ArrayList<Partition>(),now);
    }

    public static Plan plan(ReactionContext action,ClusterInfo clusterInfo,List<Partition> part,long now) throws IOException {
        float timePerByte = action.getTimePerByte();
        long remainBytes = action.getRemainBytes();
        
        if ( timePerByte == 0. || Float.isInfinite(timePerByte) || Float.isNaN(timePerByte) ) {
            // FIXME 5e-7f is processing 128MB of data in a minute
            LOG.info("no task has been completed. computing time per byte from on-going tasks");
            timePerByte = action.getJob().getAverageTimePerByte(action.getTaskType(),5e-7f);
            action.setTimePerByte(timePerByte);
        }
        float totalTime = timePerByte * remainBytes;
        
        LOG.info("time per byte = "+timePerByte+"/remain bytes = "+remainBytes+"/total time = "+totalTime);

        
        PlanType type = action.getPlanType();
        
        // FIXME include overhead
        // FIXME choose algorithm

        OptEstimate est = null;
        if ( action.getPlanSpec().requireClusterInfo() ) {
            if ( action.getPlanSpec().requireCosts() && part.size() > 0 ) {
                LOG.info("setup assign costs for each partition");
                Estimator estimator = SimpleCostModel.getInstance(action);
                action.setEstimator(estimator);
                totalTime = (float) estimator.prepare(part);
                LOG.info("total time updated by cost model = "+totalTime);
            }
            est = getEstimatedOptimalTime(clusterInfo.availList,totalTime);
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("optimal estimated runtime = "+est);
            }
        }
        
        Plan plan = specs.get(type).plan(action, clusterInfo, part, est);
        plan.setPlannedAt(now);
        action.setPlan(plan);
        
        return plan;
    }
    
    /*
    public static Plan plan(JobInProgress jip,TaskID taskid,ClusterInfo clusterInfo,List<Partition> part,long now,ReactionContext action) throws IOException {
        float timePerByte = action.getTimePerByte();
        long remainBytes = action.getRemainBytes();
        
        float totalTime = timePerByte * remainBytes;
        if ( ! part.isEmpty() ) {
            long totalBytes = 0;
            for ( Partition p : part ) {
                totalBytes += p.getLength();
            }
            totalTime = totalBytes * timePerByte;
        }
        
        PlanType type = jip.getConfiguration().getEnum(SKEWTUNE_REPARTITION_STRATEGY, PlanType.REHASH);
        
        // FIXME include overhead
        // FIXME choose algorithm
        
        OptEstimate est = getEstimatedOptimalTime(clusterInfo.availList,totalTime);
        if ( LOG.isDebugEnabled() ) {
            LOG.debug("optimal estimated runtime = "+est);
        }
        
        Plan plan = specs.get(type).plan(jip, taskid, clusterInfo, part, est, action);
        action.setPlan(plan);
        
        return plan;
    }
    */

    public static Plan plan(PlanType type,double[] availList,List<Partition> part,final float timePerByte,final long remainBytes) throws IOException {
        float totalTime = timePerByte * remainBytes;
        long bytes = 0;
        if ( ! part.isEmpty() ) {
            for ( Partition p : part ) {
                bytes += p.getLength();
            }
            totalTime = bytes * timePerByte;
        }
        
        final long totalBytes = bytes;
        
        final ReactionContext context = new ReactionContext(null,null,PartitionPlanner.getPlanSpec(type)) {
            @Override
            public <T> T getContext() { return null; }

            @Override
            public void initScanTask(Job job) throws IOException {
            }

            @Override
            public void initReactiveTask(JobInProgress newJob, Job job)
                    throws IOException {
            }
            
            @Override
            public TaskType getTaskType() { return TaskType.REDUCE; }
            
            @Override
            public String getReduceClass() {
                return "skewtune.benchmark.cloud9.wikipedia.BuildInvertedIndex$MyReducer";
            }
        };
        
        Estimator estimator = SimpleCostModel.getInstance(context);
        totalTime = (float) estimator.prepare(part);
        
        System.out.println("new total time = "+totalTime);
        // FIXME include overhead
        // FIXME choose algorithm
        OptEstimate est = getEstimatedOptimalTime(availList,totalTime);
        
        System.out.println(est);

        context.setTimePerByte(timePerByte);
        context.setRemainBytes(totalBytes);
        context.setEstimator(estimator);
        
        return specs.get(type).plan(context, new ClusterInfo(TaskType.REDUCE, availList.length, 0, 0, availList, availList.length), part, est);
    }

    static EnumMap<PlanType,PlanSpec> specs;
    static {
        synchronized (PartitionPlanner.class) {
            specs = new EnumMap<PlanType,PlanSpec>(PlanType.class);
            specs.put(PlanType.REHASH, new PlanRehash());
            specs.put(PlanType.FIRST, new PlanGreedyFirst());
            specs.put(PlanType.MAXSLOTS, new PlanMaxSlots());
            specs.put(PlanType.LONGEST, new PlanGreedyLongest());
            specs.put(PlanType.LONGEST2, new PlanGreedyLongest2());
            specs.put(PlanType.LONGEST_MODEL, new PlanGreedyLongestModel());
            specs.put(PlanType.LINEAR, new PlanGreedyLinear());
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////////////
    
    
    static class PlanGreedyLongestModel implements PlanSpec {
        @Override
        public PlanType getType() {
            return PlanType.LONGEST_MODEL;
        }

        @Override
        public boolean requireScan() {
            return true;
        }
        
        @Override
        public boolean requireClusterInfo() {
            return true;
        }

        @Override
        public Plan plan(final ReactionContext context, final ClusterInfo clusterInfo, final List<Partition> part, OptEstimate est) throws IOException {
            double[] availList = clusterInfo.availList;
            double optTime = est.getTime();
            int slots = est.getSlots();
            final List<Range> boundary = new ArrayList<Range>();
            
            BitSet allocMap = new BitSet(part.size());
            allocMap.set(0, part.size());
            
            List<Partition> sorted = new ArrayList<Partition>(part);
            Collections.sort(sorted,new Comparator<Partition>() {
                @Override
                public int compare(Partition o1, Partition o2) {
                    int sig = (int)Math.signum(o2.getCost() - o1.getCost());
                    return sig == 0 ? o1.getIndex() - o2.getIndex() : sig;
                }});
            
            LinkedList<_range> fragments = new LinkedList<_range>();
            fragments.add(new _range(part));
            
            IncrementalEstimator estimator = context.getEstimator().getIncrementalEstimator();            
            double maxTime = optTime;
            int from = 0;
            
            for ( int i = 0; i < slots && from < sorted.size() && ! allocMap.isEmpty(); ++i ) {
                double targetTime = optTime - availList[i];
                int index = sorted.get(from).getIndex();
                while ( ! allocMap.get(index) && from < sorted.size() ) {
                    index = sorted.get(++from).getIndex();
                }
                
                // good. now we found the next starting point
                ListIterator<_range> li = fragments.listIterator();
                _range rx = null;
                while ( li.hasNext() ) {
                    rx = li.next();
                    if ( rx.contains(index) ) break;
                }
                
                if ( rx == null ) {
                    // should not happen!
                    throw new IllegalStateException();
                }
                
                // lookup the sublist of part that contains the starting point
                Range r = segmentByCost(rx.getList(), rx.relative(index), optTime - availList[i], maxTime - availList[i], estimator, true);
                double newTime = availList[i] + r.getTotalTime();
                if ( maxTime < newTime ) {
                    maxTime = newTime;
                }
                boundary.add(r);
                allocMap.clear(r.begin(),r.end());

                System.err.println(r);
                System.err.println(estimator);

                // add sublist map
                _range[] xx = rx.subtract(part,r.begin(),r.end());
                li.remove(); // update current list
                if ( xx != null ) {
                    if ( xx.length > 1 ) li.add(xx[1]);
                    li.add(xx[0]);
                }
            }
            
            if ( ! fragments.isEmpty() ) {
                for ( _range r : fragments ) {
                    long sz = 0;
                    estimator.reset();
                    for ( Partition p : r.getList() ) {
                        sz += p.getLength();
                        estimator.add(p);
                    }
                    boundary.add(new Range(r.begin(), r.end(), sz, estimator.getCost()));
                    System.err.println(boundary.get(boundary.size()-1));
                    System.err.println(estimator);
                }
            }
            
            // sort by total time and generate mapping (from largest (0) to smallest (N))
            ArrayList<Range> sortedRanges = new ArrayList<Range>(boundary);
            Collections.sort(sortedRanges,new Comparator<Range>(){
                @Override
                public int compare(Range o1, Range o2) {
                    return (int)Math.signum(o2.getTotalTime() - o1.getTotalTime());
                }});
            
            // now set ordering
            for ( int i = 0; i < boundary.size(); ++i ) {
                sortedRanges.get(i).setIndex(i);
            }
            
            // now should sort in range
            Collections.sort(boundary, new Comparator<Range>() {
                @Override
                public int compare(Range o1, Range o2) {
                    int rc = o1.begin() - o2.begin();
                    return rc == 0 ? o1.getLength() - o2.getLength() : rc;
                }});
            
            if ( LOG.isDebugEnabled() ) {
                LOG.debug("new estimated time = "+maxTime);
                LOG.debug("partition boundaries = "+boundary.toString());
            }

            System.err.println(boundary.toString());
            
            final int nSlots = boundary.size();
            
            if ( context.getTaskType() == TaskType.MAP ) {
                final FileSplit split = context.getContext();
//                final List<?> splits = PartitionPlanner.getSplits(context.getConfiguration(), split, part, boundary);

                return new ReactiveMapPlan() {
                    @Override
                    public int getNumPartitions() {
                        return nSlots;
                    }

                    @Override
                    public List<?> getSplits() throws IOException {
                        return getSplits(context.getConfiguration(), split, part, boundary);
                    }
                   
                    @Override
                    protected double[] getExpectedTimes() {
                        return getExpectedTimes(clusterInfo, boundary);
                    }
                };
            } else {
                final BitSet excludes = new BitSet(boundary.size()-1);
                final List<byte[]> result = new ArrayList<byte[]>( boundary.size()-1 );
                for ( int i = 1; i < boundary.size(); ++i ) {
                    Range r = boundary.get(i);
                    MapOutputPartition partition = ((MapOutputPartition)part.get(r.begin()));
                    if ( partition.isMinKeyExcluded() ) {
                        excludes.set(result.size());
                    }
                    byte[] key = partition.getMinKey();
                    result.add(key);
                }

                return new ReactiveReducePlan() {
                    @Override
                    public int getNumPartitions() {
                        return nSlots;
                    }
    
                    @Override
                    public void write(DataOutput out) throws IOException {
                        for ( byte[] key : result ) {
                            WritableUtils.writeVInt(out, key.length);
                            out.write(key);
                        }
                        for ( Range r : boundary ) {
                            WritableUtils.writeVInt(out, r.getIndex());
                        }
                        int nexcludes = excludes.cardinality();
                        WritableUtils.writeVInt(out, nexcludes);
                        if ( nexcludes > 0 ) {
                            for (int i = excludes.nextSetBit(0); i >= 0; i = excludes.nextSetBit(i+1)) {
                                WritableUtils.writeVInt(out, i);
                            }
                        }
                    }
    
                    @Override
                    public String getPartitionClassName(boolean newApi) {
                        return skewtune.mapreduce.lib.partition.RangePartitioner.class.getName();
                    }
                    
                    @Override
                    public String toString() {
                        StringBuilder buf = new StringBuilder();
                        buf.append("Boundary:\n");
                        int i = 0;
                        for ( Range r : boundary ) {
                            buf.append(r).append("  ").append(clusterInfo.availList[i] + r.getTotalTime()).append('\n');
                        }
                        buf.append("Keys:\n");
                        for ( byte[] key : result ) {
                            buf.append(Utils.toHex(key));
                            if ( excludes.get(i) ) {
                                buf.append(" excluded");
                            }
                            buf.append('\n');
                            ++i;
                        }
                        return buf.toString();
                    }

                    @Override
                    protected double[] getExpectedTimes() {
                        return getExpectedTimes(clusterInfo, boundary);
                    }
                };
            }
        }

        @Override
        public boolean requireCosts() {
            return true;
        }
    }
    
    public static void main(String[] args) throws Exception {
        int i = 0;
        boolean isLocal = false;
        PlanType strategy = PlanType.LONGEST;
        float tpb = 0.f;
        double[] availList = new double[0];
        long bytes = 0;
        
        for ( ; i < args.length; ++i ) {
            String arg = args[i];
            if ( arg.charAt(0) != '-' ) break;
            
            if ( arg.equals("-l") || arg.equals("-local") ) {
                isLocal = true;
            } else if ( arg.equals("-slot") ) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(args[++i])));
                ArrayList<Double> tmpList = new ArrayList<Double>();
                String line = reader.readLine();
                int numSlots = Integer.parseInt(line);
                availList = new double[numSlots];
                int j = 0;
                while ( (line = reader.readLine()) != null ) {
                    availList[j++] = Double.parseDouble(line);
                }
                reader.close();
            } else if ( arg.equals("-tpb") ) {
                tpb = Float.parseFloat(args[++i]);
            } else if ( arg.equals("-strategy") ) {
                strategy = Enum.valueOf(PlanType.class, args[++i]);
            } else if ( arg.equals("-bytes") ) {
                bytes = Long.parseLong(args[++i]);
            } else {
                System.err.println("unknown option: "+arg);
            }
            
        }
        Path file = new Path(args[i]);
        
        Configuration conf = new Configuration();
        FileSystem fs = isLocal ? FileSystem.getLocal(conf) : file.getFileSystem(conf);
        
        List<Partition> parts = PartitionMapOutput.loadPartitionFile(fs,file,conf);
        for ( Partition p : parts ) {
            System.out.println(p);
        }
        Plan p = plan(strategy,availList,parts,tpb,bytes);
        System.out.println(p);
    }
}
