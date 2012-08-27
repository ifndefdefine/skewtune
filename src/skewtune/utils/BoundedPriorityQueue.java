package skewtune.utils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * keep only bounded number of items
 * @author yongchul
 *
 * @param <T>
 */
public class BoundedPriorityQueue<T extends Comparable<? super T>> implements Iterable<T> {
    private final PriorityQueue<T> q;
    private final int bound;
    private final Comparator<? super T> comp;
    private T minObj;
    
    public BoundedPriorityQueue(int n) {
        q = new PriorityQueue<T>(n+1);
        comp = new DefaultComparator<T>();
        bound = n;
    }
    
    @SuppressWarnings("unchecked")
    public BoundedPriorityQueue(int n,Comparator<? super T> comparator) {
        q = new PriorityQueue<T>(n+1,comparator);
        comp = (Comparator<? super T>) (( q.comparator() == null ) ? new DefaultComparator<T>() : q.comparator());
        bound = n;
    }
    
    public void add(T e) {
        if ( minObj != null && q.size() == bound && comp.compare(minObj,e) >= 0 ) {
            // no need to keep it
            return;
        }
        
        q.add(e);
        
        if ( q.size() > bound ) {
            // should drop one
            q.remove();
        }
        
        minObj = q.peek(); // update minObj
    }

    @Override
    public Iterator<T> iterator() {
        return q.iterator();
    }
    
    public void clear() {
        q.clear();
        minObj = null;
    }
    
    public T peek() {
        return minObj;
    }
    
    public boolean isFull() {
        return bound == q.size();
    }
    
    public Comparator<? super T> comparator() {
        return comp;
    }
    
    public int capacity() {
        return bound;
    }
    
    public int size() {
        return q.size();
    }
    
    public <T> T[] toArray(T[] a) {
        return q.toArray(a);
    }
    
    private static class DefaultComparator<T extends Comparable<? super T>> implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            return o1.compareTo(o2);
        }
    }
}
