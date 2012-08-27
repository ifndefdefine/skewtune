package skewtune.utils;

import java.util.Arrays;

/**
 * just retain last data items in given capacities.
 * each item is associated with weight
 * 
 * @author yongchul
 *
 */
public final class CircularWeightBuffer extends CircularBuffer {
	double[] weight;
	volatile long pseudoItems; // sum of weights
	long nextUpdate;
	
	public CircularWeightBuffer() {
		this(128);
	}
	
	public CircularWeightBuffer(int n) {
		super(n);
		weight = new double[data.length];
		nextUpdate = data.length;
	}
	
	/**
	 * write <param>v</param> <param>n</param> times. i.e., the weight of v is n.
	 * @param v
	 * @param n
	 * @return
	 */
	@Override
	public synchronized boolean add(double v,long n) {
		data[pos] = v;
		weight[pos] = n;
		++pos;
		++numItems;
		pseudoItems += n;
		pos &= mask;

		if ( pseudoItems >= nextUpdate ) {
			nextUpdate = pseudoItems + mask + 1;
			return true;
		}
		return false;
	}
	
	@Override
	public long getNumItems() {
		return pseudoItems;
	}

	@Override
	public synchronized Instances snapshot() {
		return new Instances(data,weight,( numItems > mask ) ? data.length : numItems);
	}
	
	@Override
	public Instances snapshotFromMark() {
		int buffered = (int)(numItems - mark);
		if ( buffered >= data.length ) {
			return new Instances(data);
		} else {
			return new Instances(data,weight,(int)(mark&mask),pos);
		}
	}
}
