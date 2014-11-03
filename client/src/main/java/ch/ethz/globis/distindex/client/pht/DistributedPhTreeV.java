package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexIterator;
import ch.ethz.globis.pht.*;

import java.util.List;

/**
 * Represents a distributed key-value PH Tree. It also conforms to the PH-Tree interfaces.
 * @param <V>                                   The type of the value.
 */
public class DistributedPhTreeV<V> implements PhTreeV<V> {

    /** Proxy to the remote PH tree */
    private DistributedPHTreeProxy<V> proxy;

    /**
     * Main constructor, avoids responsibility of building the proxy.
     * @param proxy
     */
    public DistributedPhTreeV(DistributedPHTreeProxy<V> proxy) {
        this.proxy = proxy;
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getNodeCount() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public PhTreeQStats getQuality() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public PhTree.Stats getStats() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public PhTree.Stats getStatsIdealNoNode() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public V put(long[] key, V value) {
        return proxy.put(key, value);
    }

    @Override
    public boolean contains(long... key) {
        return proxy.contains(key);
    }

    @Override
    public V get(long... key) {
        return proxy.get(key);
    }

    @Override
    public V remove(long... key) {
        return proxy.remove(key);
    }

    @Override
    public String toStringPlain() {
        return "This PhTree instance is distributed. This method is not currently implemented for a distributed PhTree.";
    }

    @Override
    public String toStringTree() {
        return "This PhTree instance is distributed. This method is not currently implemented for a distributed PhTree.";
    }

    @Override
    public PVIterator<V> queryExtent() {
        return new DistributedPhTreeIterator<>(proxy.iterator());
    }

    @Override
    public PVIterator<V> query(long[] longs, long[] longs2) {
        return new DistributedPhTreeIterator<>(proxy.query(longs, longs2));
    }

    @Override
    public boolean isRangeEmpty(long[] longs, long[] longs2) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getDIM() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getDEPTH() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public List<long[]> nearestNeighbour(int k, long... key) {
        return proxy.getNearestNeighbors(key, k);
    }

    public static class DistributedPhTreeIterator<V> implements PVIterator<V> {

        private IndexIterator<long[], V> proxyIterator;

        public DistributedPhTreeIterator(IndexIterator<long[], V> proxyIterator) {
            this.proxyIterator = proxyIterator;
        }

        @Override
        public long[] nextKey() {
            return proxyIterator.next().getKey();
        }

        @Override
        public V nextValue() {
            return proxyIterator.next().getValue();
        }

        @Override
        public PVEntry<V> nextEntry() {
            IndexEntry<long[], V> currentEntry = proxyIterator.next();
            return new PVEntry<>(currentEntry.getKey(), currentEntry.getValue());
        }

        @Override
        public boolean hasNext() {
            return proxyIterator.hasNext();
        }

        @Override
        public V next() {
            return proxyIterator.next().getValue();
        }
    }
}
