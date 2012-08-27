package skewtune.utils;

import java.util.Arrays;

/**
 * the name is inspired from WEKA
 * @author yongchul
 *
 */
public final class Instances {
	double[] data;
	double[] weight;
	int numItems;
	
	public Instances(double[] data) {
		this(data,null,data.length);
	}
	public Instances(double[] data,long n) {
		this(data,null,n);
	}
	public Instances(double[] data,double[] weight) {
		this(data,weight,data.length);
	}
	public Instances(double[] data,double[] weight,long n) {
		this.data = data.clone();
		if ( weight != null ) {
			this.weight = weight.clone();
		}
		this.numItems = (int)n;
	}
	public Instances(double[] data,int from,int to) {
		this(data,null,from,to);
	}
	public Instances(double[] data,double[] weight,int from,int to) {
		if ( from <= to ) {
			this.data = Arrays.copyOfRange(data, from, to);
			if ( weight != null ) {
				this.weight = Arrays.copyOfRange(weight, from, to);
			}
			this.numItems = to - from;
		} else {
			// should copy in two steps
			this.numItems = to + data.length - from;
			this.data = new double[this.numItems];
			System.arraycopy(data, 0, this.data, 0, to);
			System.arraycopy(data, from, this.data, to, data.length - from );
			if ( weight != null ) {
				this.weight = new double[this.numItems];
				System.arraycopy(weight, 0, this.weight, 0, to);
				System.arraycopy(weight, from, this.weight, to, weight.length - from );
			}
		}
	}
	
	public int size() {
		return numItems;
	}
	
	public double[] getData() {
		return data;
	}
	
	public double[] getWeights() {
		return weight;
	}
	
	public double getData(int i) {
		return data[i];
	}
	
	public double getWeight(int i) {
		return ( weight == null ) ? 1.0 : weight[i];
	}
	
	public boolean hasWeight() { return weight != null; }
}
