package skewtune.mapreduce.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.BitSet;

public class PrimeTable {
    private static BitSet table;
    
    static {
        if ( table == null ) {
            synchronized (PrimeTable.class) {
                if ( table == null ) {
                    InputStream in = null;
                    ObjectInputStream ois = null;
                    try {
                        in = PrimeTable.class.getResourceAsStream("primenumbers.bin");
                        if ( in != null ) {
                            ois = new ObjectInputStream(in);
                            table = (BitSet)ois.readObject();
                        }
                    } catch ( IOException ex ) {
                        table = null;
                    } catch (ClassNotFoundException e) {
                        table = null;
                        // should not happen
                    } finally {
                        if ( ois != null ) try { ois.close(); } catch ( IOException ignore ) {}
                        if ( in != null ) try { in.close(); } catch ( IOException ignore ) {}
                        ois = null;
                        in = null;
                    }
                }
            }
        }
    }
    
    /**
     * Get smallest (pseudo) prime number greater than n.
     * @param n
     * @return
     */
    public static int getNearestPrime(int n) {
        int next = -1;
        if ( table != null && n <= table.cardinality() ) {
            next = table.nextSetBit(n);
        }
        return next > 0 ? next : BigInteger.valueOf(n).nextProbablePrime().intValue();
    }

    public static void main(String[] args) throws Exception {
        // generate prime table
        final int MAX_PRIME = 65536;
        
        BitSet bitSet = new BitSet(MAX_PRIME); // 64K
        bitSet.set(0,MAX_PRIME);
        bitSet.clear(0);
        bitSet.clear(1);
        for ( int i = 2; i < MAX_PRIME; ++i ) {
            if ( ! bitSet.get(i) ) continue;
            // good. we got a prime.
            int j = 1;
            int v = i;
            while ( (v = i*(++j)) < MAX_PRIME ) bitSet.clear(v);
        }
        
        // got all bits written. sanity check
        if ( ! bitSet.get(7) || bitSet.get(100) || ! bitSet.get(101) ) {
            System.err.println("Something is wrong?!");
            System.exit(1);
        }
        
        // now write the bitset
        
        FileOutputStream fos = new FileOutputStream("primenumbers.bin");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(bitSet);
        oos.close();
    }
}
