package ch.ethz.globis.distindex.phtree.middleware;


import ch.ethz.globis.distindex.shared.MultiDimVIndex;

import java.util.Iterator;
import java.util.List;

public class PHTreeIndex<V> implements MultiDimVIndex<V> {

    @Override
    public void put(long[] key, V value) {

    }

    @Override
    public V get(long[] key) {
        return null;
    }

    @Override
    public List<V> getRange(long[] start, long[] end) {
        return null;
    }

    @Override
    public List<V> getNearestNeighbors(long[] key, int k) {
        return null;
    }

    @Override
    public Iterator<V> iterator() {
        return null;
    }
}
