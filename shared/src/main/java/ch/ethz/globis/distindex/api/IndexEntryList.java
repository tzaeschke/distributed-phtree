package ch.ethz.globis.distindex.api;

import java.util.ArrayList;

public class IndexEntryList<K, V> extends ArrayList<IndexEntry<K, V>> {

    public IndexEntryList() {
        super();
    }
    public IndexEntryList(IndexEntry<K, V> entry) {
        super();
        add(entry);
    }

    public IndexEntryList(K key, V value) {
        super();
        add(new IndexEntry<>(key, value));
    }

    public void add(K key, V value) {
        add(new IndexEntry<>(key, value));
    }
}