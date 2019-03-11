package org.apache.flink.streaming.runtime.optimization.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by tobiasmuench on 30.01.19.
 */
public class LRUdictionary<K, V> extends LinkedHashMap<K, V> {
    private final int maxEntries;
    // load factor set to 1 because we never want to resize and rehash the dictionary
    private static final float LOAD_FACTOR = 1;


    public LRUdictionary(int maxEntries) {
        super(maxEntries, LOAD_FACTOR, true);
        this.maxEntries = maxEntries;
    }


    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxEntries;
    }
}