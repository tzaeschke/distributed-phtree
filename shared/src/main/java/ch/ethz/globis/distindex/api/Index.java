package ch.ethz.globis.distindex.api;

import java.util.Iterator;

public interface Index<K, V> {

    public V put(K key, V value);

    public V get(K key);

    public IndexEntryList<K, V> getRange(K start, K end);

    public Iterator<IndexEntry<K, V>> iterator();

    public Iterator<IndexEntry<K, V>> query(K start, K end);
}
