package ch.ethz.globis.distindex.api;

import java.util.Iterator;

public interface Index<K, V> {

    public void put(K key, V value);

    public V get(K key);

    public IndexEntryList<K, V> getRange(K start, K end);

    public IndexEntryList<K, V> getNearestNeighbors(K key, int k);

    public IndexEntryList<K, V> getBatch(K startKey, int size);

    public Iterator<IndexEntry<K, V>> iterator();
}
