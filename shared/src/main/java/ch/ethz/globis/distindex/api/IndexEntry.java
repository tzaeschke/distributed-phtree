package ch.ethz.globis.distindex.api;

public class IndexEntry<K, V> {
    private K key;
    private V value;

    public IndexEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}