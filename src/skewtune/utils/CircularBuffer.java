package skewtune.utils;

/**
 * just retain last data items in given capacities
 * @author yongchul
 *
 */
public class CircularBuffer {
	protected double[] data;
	protected int  pos;
	protected long numItems;
	protected final long mask;
	
	protected long mark;
		
	public CircularBuffer() {
		this(128);
	}
	
	public CircularBuffer(int n) {
		int newN = Integer.highestOneBit(n);
		if ( newN < n ) {
			newN <<= 1;
		}
		data = new double[newN];
		mask = newN - 1;
	}
	
	public int capacity() {
		return data.length;
	}
	
	public int size() {
		return (int) (( numItems > mask ) ? data.length : numItems);
	}
	
	public long getNumItems() {
		return numItems;
	}
	
	public boolean add(double v) {
		return add(v,1);
	}

	public synchronized Instances snapshot() {
		return new Instances(data,(int) (( numItems > mask ) ? data.length : numItems));
	}

	public synchronized boolean add(double v, long n) {
		data[pos++] = v;
		pos &= mask;
		data = data;
		++numItems;
		return pos == 0; // we filled up entire buffer!
	}
	
	public void mark() {
		mark = numItems;
	}
	
	public void reset() {
		mark = -1;
	}
	
	/**
	 * return data itmes from given position <param>from</param> to currently buffered items
	 * @param from
	 * @return
	 */
	public Instances snapshotFromMark() {
		int buffered = (int)(numItems - mark);
		if ( buffered >= data.length ) {
			return new Instances(data);
		} else {
			return new Instances(data,(int)(mark&mask),pos);
		}
	}
}
