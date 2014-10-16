package ch.ethz.globis.distindex.client;

import java.util.Iterator;
import java.util.List;

public interface Index<K, V> {

    public void put(K key, V value);

    public V get(K key);

    public List<V> getRange(K start, K end);

    public List<V> getNearestNeighbors(K key, int k);

    public Iterator<V> iterator();
}
