package ch.ethz.globis.distindex.middleware.pht;


import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.api.MultiDimVIndex;
import ch.ethz.globis.pht.*;
import ch.ethz.globis.pht.v3.PhTree3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * An adaptor to the PHTree functionality.
 *
 * @param <V>
 */
public class PHTreeIndexAdaptor<V> implements MultiDimVIndex<V> {

    private PhTreeV<V> tree;

    public PHTreeIndexAdaptor(int dim, int depth) {
        tree = new PhTree3<>(dim, depth);
    }

    @Override
    public void put(long[] key, V value) {
        tree.put(key, value);
    }

    @Override
    public V get(long[] key) {
        return tree.get(key);
    }

    @Override
    public IndexEntryList<long[], V> getRange(long[] start, long[] end) {
        PhKVIterator<V> it = tree.query(start, end);
        return iteratorToList(it);
    }

    @Override
    public IndexEntryList<long[], V> getNearestNeighbors(long[] key, int k) {
        ArrayList<PhEntry<V>> list = tree.nearestNeighbour(k, key);
        return entriesToList(list);
    }

    @Override
    public Iterator<IndexEntry<long[], V>> iterator() {
        //FIXME implement iterator properly
        return iteratorToList(tree.queryExtent()).iterator();
    }

    private IndexEntryList<long[], V> iteratorToList(PhKVIterator<V> iterator) {
        IndexEntryList<long[], V> entries = new IndexEntryList<>();
        while (iterator.hasNext()) {
            PhEntry<V> entry = iterator.nextEntry();
            entries.add(new IndexEntry<>(entry.getKey(), entry.getValue()));
        }
        return entries;
    }

    private IndexEntryList<long[], V> entriesToList(List<PhEntry<V>> list) {
        IndexEntryList<long[], V> entries = new IndexEntryList<>();
        for (PhEntry<V> entry : list) {
            entries.add(new IndexEntry<>(entry.getKey(), entry.getValue()));
        }
        return entries;
    }
}