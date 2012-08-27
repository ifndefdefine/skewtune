package skewtune.utils;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

public class Itertools {
    static final Comparator<Object> NATURAL_ORDER = new Comparator<Object>() {
        @SuppressWarnings("unchecked")
        @Override
        public int compare(Object o1, Object o2) {
            return ((Comparable<Object>) o1).compareTo(o2);
        }
    };
    
    public static class PriorityQueue<V> implements Iterable<V>, Queue<V> {
        private static final int DEFAULT_CAPACITY = 1023;
        
        private java.util.PriorityQueue<V> q;
        
        private final Iterator<V> _i = new Iterator<V>() {
            @Override
            public boolean hasNext() {
                return q.size() > 0;
            }
    
            @Override
            public V next() {
                return q.poll();
            }
    
            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        public PriorityQueue() {
            this(DEFAULT_CAPACITY);
        }

        public PriorityQueue(int cap) {
            q = new java.util.PriorityQueue<V>(cap);
        }
        
        public PriorityQueue(Comparator<? super V> comp) {
            this(DEFAULT_CAPACITY,comp);
        }
        
        public PriorityQueue(int cap,Comparator<? super V> comp) {
            q = new java.util.PriorityQueue<V>(cap,comp);
        }
        
        public V peek() {
            return q.peek();
        }
        
        public V poll() {
            return q.poll();
        }
        
        public boolean add(V v) {
            return q.add(v);
        }
        
        public boolean offer(V v) {
            return q.offer(v);
        }
        
        public void clear() {
            q.clear();
        }
        
        public boolean addAll(Collection<? extends V> c) {
            return q.addAll(c);
        }
        
        public int size() {
            return q.size();
        }

        @Override
        public Iterator<V> iterator() {
            return _i;
        }
        
        public Iterator<V> filter(final Predicate<V> pred) {
            return new Iterator<V>() {
                @Override
                public boolean hasNext() {
                    return ! q.isEmpty() && pred.eval(q.peek());
                }

                @Override
                public V next() {
                    return q.poll();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        public boolean isEmpty() {
            return q.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return q.contains(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return q.containsAll(c);
        }

        @Override
        public boolean remove(Object o) {
            return q.remove(o);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return q.removeAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return q.retainAll(c);
        }

        @Override
        public Object[] toArray() {
            return q.toArray();
        }

        @Override
        public <T> T[] toArray(T[] arr) {
            return q.toArray(arr);
        }

        @Override
        public V element() {
            return q.element();
        }

        @Override
        public V remove() {
            return q.remove();
        }
    }
    
    public interface Predicate<V> {
        public boolean eval(V value);
    }
    
    public static class True<V> implements Predicate<V> {
        public boolean eval(V value) {
            return true;
        }
    }
    
    public interface KeyFunc<K,V> {
        public K eval(V v);
    }

    public final class IdentityKeyFunc<V> implements KeyFunc<V,V> {
        public V eval(V v) {
            return v;
        }
    }

    public static class Filter<V> implements Iterable<V>, Iterator<V> {
        final Iterator<V> i;
        final Predicate<V> pred;
        private V _current;

        public Filter(Iterable<V> i) {
            this(i.iterator());
        }
        
        public Filter(Iterable<V> i,Predicate<V> pred) {
            this(i.iterator(),pred);
        }
        
        public Filter(Iterator<V> i) {
            this(i,new True<V>());
        }
        
        public Filter(Iterator<V> i,Predicate<V> pred) {
            this.i = i;
            this.pred = pred;
        }
        
        @Override
        public Iterator<V> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            while ( i.hasNext() ) {
                _current = i.next();
                if ( pred.eval(_current) ) return true; 
            }
            _current = null; // clear
            return false;
        }

        @Override
        public V next() {
            return _current;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
    
    public static class Group<K,V> implements Iterable<V> {
        private final K group;
        private final Iterator<V> i;
        
        Group(K grp,Iterator<V> i) {
            this.group = grp;
            this.i = i;
        }
        
        public K group() {
            return group;
        }

        @Override
        public Iterator<V> iterator() {
            return i;
        }
    }
    
    public static class GroupBy<K,V> implements Iterable<Group<K,V>>,Iterator<Group<K,V>> {
        final KeyFunc<K,V> keyFunc;
        final Comparator<K> comp;
        final Iterator<V> values;

        private K currentKey;
        private V currentValue; // the one returned by next()
        private K nextKey;
        private V nextValue;
        private boolean closed;

        public GroupBy(Iterable<V> values,KeyFunc<K,V> keyFunc) {
            this(values.iterator(),keyFunc);
        }
        
        @SuppressWarnings("unchecked")
        public GroupBy(Iterator<V> values,KeyFunc<K,V> keyFunc) {
            this.values = values;
            this.keyFunc = keyFunc;
            this.comp = (Comparator<K>) NATURAL_ORDER;
        }

        public GroupBy(Iterable<V> values,KeyFunc<K,V> keyFunc,Comparator<K> comp) {
            this(values.iterator(),keyFunc,comp);
        }
        
        public GroupBy(Iterator<V> values,KeyFunc<K,V> keyFunc,Comparator<K> comp) {
            this.values = values;
            this.keyFunc = keyFunc;
            this.comp = comp;
        }
        
        @Override
        public Iterator<Group<K,V>> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            if ( closed ) return false;
            
            if ( currentKey == null ) {
                // this is the first time it is called!
                closed = ! values.hasNext();
                if ( ! closed ) {
                    nextValue = values.next();
                    nextKey = keyFunc.eval(nextValue);
                }
            } else {
                // initially currentKey is null and never be null again!
                closed = ! skip();
            }
            
            // after skip, it's in either one of the two states
            // 1. values reached to the end
            // 2. currentValue < nextValue
            if ( ! closed ) {
                currentKey = nextKey;
                currentValue = nextValue;
            }
            
            return ! closed;
        }
        
        /**
         * @return true if we have more data. false otherwise.
         */
        private boolean skip() {
            // first check whether the next key is the same as the current one
            boolean equalKey = false;
            while ( (equalKey = comp.compare(currentKey,nextKey) == 0) && values.hasNext() ) {
                nextValue = values.next();
                nextKey = keyFunc.eval(nextValue);
            }
            
            // now closely check with end condition
            // if currentKey == last nextKey, then the while loop terminates due to values.hasNext(). EOS.
            // otherwise, currentKey != nextKey which means we have something to iterate on
            
            return ! equalKey;
        }


        @Override
        public Group<K,V> next() {
            return new Group<K,V>(currentKey,new Iterator<V>() {
                boolean _closed;
                boolean firstValue = true;
                // in the beginning, currentValue == nextValue
                
                
                @Override
                public boolean hasNext() {
                    if ( firstValue ) {
                        return true;
                    }

                    if ( ! _closed ) {
                        if ( values.hasNext() ) {
                            nextValue = values.next();
                            nextKey = keyFunc.eval(nextValue);
                            _closed = comp.compare(currentKey,nextKey) != 0;
                            if ( ! _closed ) {
                                currentValue = nextValue;
                            }
                        } else {
                            _closed = true;
                        }
                    }
                    return ! _closed;
                }

                @Override
                public V next() {
                    if ( _closed )
                        throw new NoSuchElementException();
                    firstValue = false;
                    return currentValue;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            });
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
