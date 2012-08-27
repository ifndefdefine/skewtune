package skewtune.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.stat.regression.OLSMultipleLinearRegression;
import org.apache.hadoop.mapreduce.TaskType;

import skewtune.mapreduce.JobInProgress.ReactionContext;
import skewtune.mapreduce.PartitionMapInput.MapInputPartition;
import skewtune.mapreduce.PartitionMapOutput.MapOutputPartition;
import skewtune.mapreduce.PartitionPlanner.Partition;

public class SimpleCostModel {
    public static final Log LOG = LogFactory.getLog(PartitionPlanner.class);
    
    public static interface IncrementalEstimator {
        public void add(Partition p);
        public double cost(Partition p);
        public double getCost();
        public void reset();
    }
    
    public static abstract class Estimator {
        protected final double[] beta;
        
        protected Estimator(double[] model) {
            beta = model == null ? null : model.clone();
        }
        public abstract double cost(Partition p);
        public abstract double prepare(List<Partition> partitions);
        public IncrementalEstimator getIncrementalEstimator() {
            throw new UnsupportedOperationException();
        }
    }
    
    public static class TimePerByteEstimator extends Estimator {
        final double tpb;
        public TimePerByteEstimator(double tpb) {
            super(null);
            this.tpb = tpb;
        }

        @Override
        public double cost(Partition p) {
            return p.getLength() * tpb;
        }

        @Override
        public double prepare(List<Partition> partitions) {
            long totalBytes = 0;
            for ( Partition p : partitions ) {
                long len = p.getLength();
                p.setCost(tpb*len);
                totalBytes += len;
            }
            return totalBytes * tpb;
        }
        
        @Override
        public IncrementalEstimator getIncrementalEstimator() {
            return new IncrementalEstimator() {
                long bytes;
                
                @Override
                public void add(Partition p) {
                    bytes += p.getLength();
                }

                @Override
                public double cost(Partition p) {
                    return bytes*tpb;
                }

                @Override
                public double getCost() {
                    return bytes*tpb;
                }

                @Override
                public void reset() {
                    bytes = 0;
                }
                
                @Override
                public String toString() {
                    return String.format("%d bytes %.3fs",bytes,bytes*tpb);
                }
            };
        }
    }
    
    public static class ReduceEstimator extends Estimator {
        public ReduceEstimator(double[] beta) { super(beta); }
        
        @Override
        public double cost(Partition p) {
            MapOutputPartition mp = (MapOutputPartition)p;
            // FIXME
//            int kg = mp.isSingleton() ? 1 : mp.getDistinctKeys();
            int kg = 0;
            return beta[0] + beta[1] * mp.getNumRecords() + beta[2] * mp.getLength() + beta[3] * kg;
        }

        @Override
        public double prepare(List<Partition> partitions) {
            // FIXME this is only used for total cost evaluation. we are cheating!
            int nrecs = 0;
            long bytes = 0;
            int keys = 0;
            for ( Partition x : partitions ) {
                double c = cost(x);
                x.setCost(c);
                
                MapOutputPartition other = (MapOutputPartition)x;
                nrecs += other.numRecs;
                bytes += other.totBytes;
//                keys += other.distKeys;
//                add((MapOutputPartition)x);
            }
            
            if ( LOG.isInfoEnabled() ) {
                LOG.info("# records = "+nrecs+"; # bytes = "+bytes+"; # keys = "+keys);
            }
            
            return beta[0] + beta[1] * nrecs + beta[2] * bytes + beta[3] * keys;
        }

        @Override
        public IncrementalEstimator getIncrementalEstimator() {
            return new IncrementalEstimator() {
                final double[] myBeta = beta.clone();
                int nrecs;
                long bytes;
                int keys;

                @Override
                public void add(Partition p) {
                    MapOutputPartition x = (MapOutputPartition)p;
                    nrecs += x.numRecs;
                    bytes += x.totBytes;
                    // FIXME
//                    keys += x.distKeys;
                }

                @Override
                public double cost(Partition p) {
                    MapOutputPartition x = (MapOutputPartition)p;
                    // FIXME
//                    return myBeta[0] + myBeta[1] *(x.numRecs+ nrecs) + myBeta[2] * (x.totBytes+bytes) + myBeta[3] * (x.distKeys + keys);
                    return myBeta[0] + myBeta[1] *(x.numRecs+ nrecs) + myBeta[2] * (x.totBytes+bytes) + myBeta[3] * (keys);
                }

                @Override
                public double getCost() {
                    return myBeta[0] + myBeta[1] * nrecs + myBeta[2] * bytes + myBeta[3] * keys;
                }

                @Override
                public void reset() {
                    nrecs = 0;
                    bytes = 0;
                    keys = 0;
                }
                
                @Override
                public String toString() {
                    return String.format("recs=%d,bytes=%d,keys=%d %.3fs",nrecs,bytes,keys,getCost());
                }
            };
        }
    }
    
    public static class MapEstimator extends Estimator {
        public MapEstimator(double[] beta) { super(beta); }

        @Override
        public double cost(Partition p) {
            MapInputPartition mp = (MapInputPartition)p;
            return beta[0] + beta[1] * mp.getNumRecords() + beta[2] * mp.getLength();
        }

        @Override
        public double prepare(List<Partition> partitions) {
            long offset = ((MapInputPartition)partitions.get(0)).offset;
            int nrecs = 0;
            for ( Partition x : partitions ) {
                double c = cost(x);
                if ( c < 0. ) {
                    throw new IllegalStateException(x + ": esitmated cost is zero?");
                }
                x.setCost(c);
                nrecs += ((MapInputPartition)x).numRecs;
            }
            long lastOffset = ((MapInputPartition)partitions.get(partitions.size()-1)).lastOffset;
            return beta[0] + beta[1] * nrecs + beta[2] * (lastOffset - offset);
        }
        
        @Override
        public IncrementalEstimator getIncrementalEstimator() {
            return new IncrementalEstimator() {
                long length;
                int nrecs;
                final double[] myBeta = beta.clone();
                
                @Override
                public void add(Partition p) {
                    MapInputPartition x = (MapInputPartition)p;
                    length += p.getLength();
                    nrecs += x.getNumRecords();
                }

                @Override
                public double cost(Partition p) {
                    MapInputPartition x = (MapInputPartition)p;
                    return myBeta[0] + myBeta[1] * (x.getNumRecords()+nrecs) + myBeta[2] * (length+x.getLength());
                }

                @Override
                public double getCost() {
                    return myBeta[0] + myBeta[1] * nrecs + myBeta[2] * length;
                }

                @Override
                public void reset() {
                    length = 0;
                    nrecs = 0;
                }
            };
        }
    }
    
    static Map<String,Estimator> _costModels;
    
    static {
        _costModels = new HashMap<String,Estimator>();
        _costModels.put("cloudBurst.MerReduce$MapClass", new MapEstimator(new double[] { 8.115247748585846, 0.0013161168478274866, 4.5103178080472315E-6 }));
        _costModels.put("cloudBurst.MerReduce$ReduceClass", new ReduceEstimator(new double[] { 2.6624234938039546, 1.6431948141574807E-4, 2.6694212629771995E-6, -0.12333674468115648 }));
        _costModels.put("edu.umd.cloud9.example.pagerank.RunPageRankBasic$MapClass", new MapEstimator(new double[] { 4.598884940525774, -4.461940108909357E-5, 2.159726314480998E-6 }));
        _costModels.put("edu.umd.cloud9.example.pagerank.RunPageRankBasic$ReduceClass", new ReduceEstimator(new double[] { 2.7861693518420036, 2.402387996330705E-6, 9.11395719728384E-8, 1.616892572690737E-7 }));
        _costModels.put("skewtune.benchmark.cloud9.wikipedia.BuildInvertedIndex$MyMapper", new MapEstimator(new double[] { 10.384695272782304, 9.97486824784973E-5, 7.5473682997536E-7 }));
        _costModels.put("skewtune.benchmark.cloud9.wikipedia.BuildInvertedIndex$MyReducer", new ReduceEstimator(new double[]{ 4.289755918791618, 1.364307516502848E-6, 1.4207747894800385E-7, 0.009913639536569388 }));
    }
    
    public static Estimator getInstance(ReactionContext context) {
        String clsName = context.getTaskType() == TaskType.MAP ? context.getMapClass() : context.getReduceClass();
        Estimator estimator = _costModels.get(clsName);
        if ( LOG.isInfoEnabled() ) {
            LOG.info("estimator for class "+clsName+" "+(estimator == null ? "(no preset)" : estimator.getClass()));
        }
        return estimator == null ? new TimePerByteEstimator(context.getTimePerByte()): estimator;
    }
    
    public static interface Model {
        public Estimator buildModel();
        public int size();
        public void clear();
    }
    
    public static class MapCostModel implements Model {
//        double[] arr = new double[4096]; // keep 1000 samples
        double[] arr = new double[16384];
        int obs;
        int modCount;
        
        public void add(double t,int recs,long bytes) {
            if ( obs >= arr.length ) {
                obs = 0; // overwrite
            }
            int i = obs * 3;
            arr[i] = t;
            arr[++i] = recs;
            arr[++i] = bytes;
            ++obs;
            ++modCount;
        }
        
        @Override
        public Estimator buildModel() {
            OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
            double[] buf = modCount >= (arr.length /3)? arr : Arrays.copyOf(arr,  obs*3);
            regression.newSampleData(buf,obs,2);
            
            double[] beta = regression.estimateRegressionParameters();
            if ( LOG.isInfoEnabled() ) {
                double[] residuals = regression.estimateResiduals();
                double[][] parametersVariance = regression.estimateRegressionParametersVariance();
                double regressandVariance = regression.estimateRegressandVariance();
                double rSquared = regression.calculateRSquared();
                double sigma = regression.estimateRegressionStandardError();
            
                LOG.info("beta = "+Arrays.toString(beta));
                LOG.info("residuals = "+Arrays.toString(residuals));
                LOG.info("Parameter Variance:");
                for ( double[] vars : parametersVariance ) {
                    LOG.info(Arrays.toString(vars));
                }
                LOG.info("Regression variance = "+regressandVariance);
                LOG.info("R Squared = "+rSquared);
                LOG.info("sigma = "+sigma);
            }
            
            return new ReduceEstimator(beta);
        }

        @Override
        public int size() {
            return modCount;
        }
        
        @Override
        public void clear() {
            modCount = 0;
            obs = 0;
        }
    }

    
    public static class ReduceCostModel implements Model {
//        double[] arr = new double[4096]; // keep 1000 samples
        double[] arr = new double[16384];
        int obs;
        int modCount;
        
        public void add(double t,int kg,int recs,long bytes) {
            if ( obs >= arr.length ) {
                obs = 0; // overwrite
            }
            int i = obs << 2;
            arr[i] = t;
            arr[++i] = recs;
            arr[++i] = bytes;
            arr[++i] = kg;
            ++obs;
            ++modCount;
        }
        
        @Override
        public Estimator buildModel() {
            OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
            double[] buf = modCount >= (arr.length>>>2) ? arr : Arrays.copyOf(arr, obs<<2);
            regression.newSampleData(buf,obs,3);
            
            double[] beta = regression.estimateRegressionParameters();
            if ( LOG.isInfoEnabled() ) {
                double[] residuals = regression.estimateResiduals();
                double[][] parametersVariance = regression.estimateRegressionParametersVariance();
                double regressandVariance = regression.estimateRegressandVariance();
                double rSquared = regression.calculateRSquared();
                double sigma = regression.estimateRegressionStandardError();
            
                LOG.info("beta = "+Arrays.toString(beta));
                LOG.info("residuals = "+Arrays.toString(residuals));
                LOG.info("Parameter Variance:");
                for ( double[] vars : parametersVariance ) {
                    LOG.info(Arrays.toString(vars));
                }
                LOG.info("Regression variance = "+regressandVariance);
                LOG.info("R Squared = "+rSquared);
                LOG.info("sigma = "+sigma);
            }
            
            return new ReduceEstimator(beta);
        }

        @Override
        public int size() {
            return modCount;
        }

        @Override
        public void clear() {
            modCount = 0;
            obs = 0;
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        String fn = args[0];
        BufferedReader reader = new BufferedReader(new FileReader(fn)); 
        
        MapCostModel mcostModel = new MapCostModel();
        ReduceCostModel rcostModel = new ReduceCostModel();
        
        String prevJob = "";
        String prevClass = "";
        String line;
        while ( (line = reader.readLine() ) != null ) {
            String[] flds = line.split(",");
            String jtidStr = flds[0];
            String jobidStr = flds[1];
            
            String currentJobid = jtidStr + "_" + jobidStr;
            
            String taskTypeStr = flds[2];
            String taskidStr = flds[3];
            double t = Double.parseDouble(flds[4]);
            long bytes = Long.parseLong(flds[5]);
            int recs = Integer.parseInt(flds[6]);
            int groups = flds[7].length() == 0 ? 0 : Integer.parseInt(flds[7]);
            String clsName = flds[8];
            
            if ( ! prevClass.equals(clsName) ) {
                if ( mcostModel.size() > 0 ) {
                    System.err.println(prevJob + ":" + prevClass + " model");
                    mcostModel.buildModel();
                }
                
                if ( rcostModel.size() > 0 ) {
                    System.err.println(prevJob + ":" + prevClass + " model");
                    rcostModel.buildModel();
                }
                
                mcostModel.clear();
                rcostModel.clear();
            }
            
            if ( taskTypeStr.equals("m") ) {
                if ( bytes > 0 && recs > 0 )
                mcostModel.add(t, recs, bytes);
            } else {
                if ( bytes > 0  && groups > 0 && recs > 0 )
                rcostModel.add(t, groups, recs, bytes);
            }
            
            
            prevJob = currentJobid;
            prevClass = clsName;
        }
        reader.close();
        
        if ( mcostModel.size() > 0 ) {
            System.err.println(prevJob + ":" + prevClass + " model");
            mcostModel.buildModel();
        }
        
        if ( rcostModel.size() > 0 ) {
            System.err.println(prevJob + ":" + prevClass + " model");
            rcostModel.buildModel();
        }
//        ReduceCostModel costModel = new ReduceCostModel();
//        costModel.add(103,9765,510412,1347196);
//        costModel.add(21,1542,95448,222292);
//        costModel.add(14,627,241010,608587);
//        costModel.add(48,3385,278938,724778);
//        costModel.add(126,10011,437846,1235265);
//        costModel.buildModel();
    }
}
