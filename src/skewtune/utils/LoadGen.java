package skewtune.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import skewtune.mapreduce.SkewTuneJobConfig;

/**
 * generate artificial disk load -- write
 * @author yongchul
 */
public class LoadGen {
    private Process proc;
    private ProcessBuilder pb;
    
    public LoadGen(Configuration conf) {
        String loadCmd = conf.get(SkewTuneJobConfig.SKEWTUNE_DEBUG_GENERATE_DISKLOAD_CMD);
        if ( loadCmd == null ) return;
        String loadAmount = conf.get(SkewTuneJobConfig.SKEWTUNE_DEBUG_GENERATE_DISKLOAD_AMOUNT,"5G");
        pb = new ProcessBuilder(loadCmd,loadAmount);
    }
    
    public void start() throws IOException, InterruptedException {
        long begin = System.currentTimeMillis();
        
        int rc1 = Runtime.getRuntime().exec("/bin/sync").waitFor();
        int rc2 = Runtime.getRuntime().exec("/usr/bin/dropCache").waitFor();
        if ( pb != null )
            proc = pb.start();
        
        long end = System.currentTimeMillis();
        
        System.err.println("sync = "+rc1+"; dropCache = "+rc2+"; delay = "+(end-begin)+"ms");
    }
    
    public int awaitCompletion() throws InterruptedException {
        if ( proc != null ) {
            return proc.waitFor();
        }
        return -1;
    }
}
