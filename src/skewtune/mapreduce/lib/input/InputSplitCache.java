package skewtune.mapreduce.lib.input;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * cache input split objects.
 * later the InputFormat object can retrieve the split objects
 */
public class InputSplitCache {
	private static final Log LOG = LogFactory.getLog(InputSplitCache.class);
	
    public static final String SPLIT_CACHE_KEY = "mapreduce.inputformat.splitcache.key";

    private static final AtomicInteger nextCacheID = new AtomicInteger();

    private static final HashMap<Integer,List<?>> cache = new HashMap<Integer,List<?>>();

    public static void clear() {
        synchronized (cache) {
            cache.clear();
        }
    }

    public static int set(Configuration conf,List<?> split) {
        int cacheKey = nextCacheID.incrementAndGet();
        synchronized (cache) {
            cache.put(cacheKey, split);
        }
        conf.setInt(SPLIT_CACHE_KEY,cacheKey);
        return cacheKey;
    }

    public static int getCacheKey(Configuration conf) {
        return conf.getInt(SPLIT_CACHE_KEY,-1);
    }

    public static <T> List<T> get(Configuration conf) {
        int key = conf.getInt(SPLIT_CACHE_KEY,-1);
        return get(key);
    }
    
    public static int getSplitSize(Configuration conf) {
        int key = conf.getInt(SPLIT_CACHE_KEY,-1);
        if ( key < 0 ) return 0;
        
        List<?> splits = null;
        synchronized (cache) {
            splits = cache.get(key);
        }
        return splits == null ? 0 : splits.size();
    }
    
    @SuppressWarnings("unchecked")
    public static <T> List<T> get(int key) {
        if ( key < 0 ) {
            LOG.warn("Cache key does not exist!: "+key);
            return Collections.emptyList();
        }

        List<T> splits = null;
        synchronized (cache) {
            splits = (List<T>)cache.get(key);
        }
        return ( splits == null ) ? (List<T>)Collections.emptyList() : splits;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> remove(int key) {
        synchronized (cache) {
            return (List<T>)cache.remove(key);
        }
    }
}
