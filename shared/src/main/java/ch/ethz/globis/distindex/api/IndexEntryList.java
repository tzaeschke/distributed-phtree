package ch.ethz.globis.distindex.api;

import java.util.ArrayList;

/**
 * An ordered list of {@link ch.ethz.globis.distindex.api.IndexEntry} objects.
 *
 * @param <K>                           The type of the index key.
 * @param <V>                           The type of the index value.
 */
public class IndexEntryList<K, V> extends ArrayList<IndexEntry<K, V>> {

    /** */
	private static final long serialVersionUID = 1L;

	public IndexEntryList() {
        super();
    }

    public IndexEntryList(int size) {
        super(size);
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