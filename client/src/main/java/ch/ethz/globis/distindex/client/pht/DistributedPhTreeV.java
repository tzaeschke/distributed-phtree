package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.pht.*;
import ch.ethz.globis.pht.util.PhMapper;
import ch.ethz.globis.pht.util.PhTreeQStats;

import java.util.List;

/**
 * Represents a distributed key-value PH Tree. It also conforms to the PH-Tree interfaces.
 * @param <V>                                   The type of the value.
 */
public class DistributedPhTreeV<V> implements PhTree<V> {

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
    public PhTreeHelper.Stats getStats() {
        return proxy.getStats();
    }

    @Override
    public PhTreeHelper.Stats getStatsIdealNoNode() {
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
        return proxy.toStringPlain();
    }

    @Override
    public String toStringTree() {
        return proxy.toStringTree();
    }

    @Override
    public PhIterator<V> queryExtent() {
        return new DistributedPhTreeIterator<>(proxy.iterator());
    }

    @Override
    public PhIterator<V> query(long[] min, long[] max) {
        return new DistributedPhTreeIterator<V>(proxy.query(min, max));
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
        return proxy.getNearestNeighbuor(i, phDistance, phDimFilter, keys);
    }

    @Override
    public V update(long[] oldKey, long[] newKey) {
        return proxy.update(oldKey, newKey);
    }

    @Override
    public List<PhEntry<V>> queryAll(long[] min, long[] max) {
        return proxy.queryAll(min, max);
    }

    @Override
    public <R> List<R> queryAll(long[] min, long[] max, int maxResults, PhPredicate filter, PhMapper<V, R> mapper) {
        return proxy.queryAll(min, max, maxResults, filter, mapper);
    }

    public PHTreeIndexProxy<V> getProxy() {
        return proxy;
    }

	@Override
	public void clear() {
		//TODO implement me
		throw new UnsupportedOperationException();
	}
}