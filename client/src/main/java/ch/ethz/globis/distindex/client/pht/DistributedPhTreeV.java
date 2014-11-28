package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.pht.*;

import java.util.List;

/**
 * Represents a distributed key-value PH Tree. It also conforms to the PH-Tree interfaces.
 * @param <V>                                   The type of the value.
 */
public class DistributedPhTreeV<V> implements PhTreeV<V> {

    /** Proxy to the remote PH tree */
    private PHTreeIndexProxy<V> proxy;

    /**
     * Main constructor, avoids responsibility of building the proxy.
     * @param proxy
     */
    public DistributedPhTreeV(PHTreeIndexProxy<V> proxy) {
        this.proxy = proxy;
    }

    @Override
    public int size() {
        return proxy.size();
    }

    @Override
    public int getNodeCount() {
        return proxy.getNodeCount();
    }

    @Override
    public PhTreeQStats getQuality() {
        return proxy.getQuality();
    }

    @Override
    public PhTree.Stats getStats() {
        return proxy.getStats();
    }

    @Override
    public PhTree.Stats getStatsIdealNoNode() {
        return proxy.getStatsIdealNoNode();
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
        //ToDo implement toStringPlain()
        //return proxy.toStringPlain();
        return "This PhTree instance is distributed. This method is not currently implemented for a distributed PhTree.";
    }

    @Override
    public String toStringTree() {
        //ToDo implement toStringTree()
        //return proxy.toStringTree();
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
        //ToDo implement this by doing a server call
        PVIterator<V> it  = query(longs, longs2);
        return !it.hasNext();
    }

    @Override
    public int getDIM() {
        return proxy.getDim();
    }

    @Override
    public int getDEPTH() {
        return proxy.getDepth();
    }

    @Override
    public List<long[]> nearestNeighbour(int k, long... key) {
        return proxy.getNearestNeighbors(key, k);
    }

    @Override
    public List<long[]> nearestNeighbour(int i, PhDistance phDistance, PhDimFilter phDimFilter, long... keys) {
        //ToDo this is currently not supported by the PH Tree, but it will change in the future
        throw new UnsupportedOperationException();
    }

    public PHTreeIndexProxy<V> getProxy() {
        return proxy;
    }
}