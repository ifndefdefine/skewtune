/**
 * 
 */
package skewtune.utils;

/**
 * optimized for single updater. other thread will see new sum and correction value.
 * @author yongchul
 *
 */
public final class KahanSum {
    double sum;
    double correction;
    double correctedAddened;

    public void add(double d) {
        correctedAddened = d + correction;
        double tempSum = sum + correctedAddened;
        correction = correctedAddened - (tempSum - sum);
        sum = tempSum;
    }

    public double value() { return sum + correction; }
    
    public void reset() {
    	sum = 0.0;
    	correction = 0.0;
    	correctedAddened = 0.0;
    }
}
