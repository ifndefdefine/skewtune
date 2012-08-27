package skewtune.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import skewtune.mapreduce.SampleMapOutput.Reduce;
import skewtune.mapreduce.SampleMapOutput.SampleInterval;

public class ReduceTest extends Reduce<Text> {
    @Before
    public void initTest() {
        groupComp = WritableComparator.get(Text.class);
    }
    
    @Override
    protected void write(Text prev, int nkeys, double nrecs, double nbytes, Text current) throws IOException {
        int nr = nrecs > 0. ? (int)Math.round(nrecs) : 0;
        long nb = nbytes > 0. ? (long)Math.round(nbytes) : 0;
        System.out.println("("+prev+","+current+"); numKeys="+nkeys+"; numRecs="+nr+"; numBytes="+nb);
    }
    
    @Override
    protected void write(Text key, double nrecs, double nbytes) throws IOException {
        int nr = nrecs > 0. ? (int)Math.round(nrecs) : 0;
        long nb = nbytes > 0. ? (long)Math.round(nbytes) : 0;
        System.out.println(key+"; numRecs="+nr+"; numBytes="+nb);
    }
    
    @Test
    public final void test() throws IOException, InterruptedException {
        List<SampleInterval<Text>> intervals = new ArrayList<SampleInterval<Text>>();
        
        intervals.add(new SampleInterval<Text>(new Text("k003"),4,8,9,9,18,new Text("k007"),3,6 ));
        intervals.add(new SampleInterval<Text>(new Text("k007"),1,2,10,10,20,new Text("k100"),2,4 ));
        intervals.add(new SampleInterval<Text>(new Text("k050"),2,4,14,14,28,new Text("k095"),5,10 ));
        
        alignKeys(intervals);
//        fail("Not yet implemented"); // TODO
    }

}
