package ch.ethz.globis.distindex.middleware.pht;


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
    public List<V> getRange(long[] start, long[] end) {
        PhKVIterator<V> it = tree.query(start, end);
        return iteratorToList(it);
    }

    @Override
    public List<V> getNearestNeighbors(long[] key, int k) {
        ArrayList<PhEntry<V>> list = tree.nearestNeighbour(k, key);
        return entriesToList(list);
    }

    @Override
    public Iterator<V> iterator() {
        return tree.queryExtent();
    }

    private List<V> iteratorToList(PhKVIterator<V> iterator) {
        List<V> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.nextValue());
        }
        return entries;
    }

    private List<V> entriesToList(List<PhEntry<V>> list) {
        List<V> entries = new ArrayList<>();
        for (PhEntry<V> entry : list) {
            entries.add(entry.getValue());
        }
        return entries;
    }
}
