/*
This file is part of PH-Tree:
A multi-dimensional indexing and storage structure.

Copyright (C) 2011-2015
Eidgenössische Technische Hochschule Zürich (ETH Zurich)
Institute for Information Systems
GlobIS Group
Bogdan Vancea, Tilmann Zaeschke
zaeschke@inf.ethz.ch or zoodb@gmx.de

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
package ch.ethz.globis.distindex.client.pht;

import java.util.List;

import ch.ethz.globis.phtree.PhDistance;
import ch.ethz.globis.phtree.PhEntry;
import ch.ethz.globis.phtree.PhFilter;
import ch.ethz.globis.phtree.PhRangeQuery;
import ch.ethz.globis.phtree.PhTree;
import ch.ethz.globis.phtree.util.PhMapper;
import ch.ethz.globis.phtree.util.PhTreeStats;

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
    public PhTreeStats getStats() {
        return proxy.getStats();
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
    public PhExtent<V> queryExtent() {
        return new DistributedPhTreeIterator2<>(proxy.iterator());
    }

    @Override
    public PhQuery<V> query(long[] min, long[] max) {
        return new DistributedPhTreeIterator<>(proxy.query(min, max));
    }

    @Override
    public int getDim() {
        return proxy.getDim();
    }

    @Override
    public int getBitDepth() {
        return proxy.getDepth();
    }

    @Override
    public PhKnnQuery<V> nearestNeighbour(int k, long... key) {
        return proxy.getNearestNeighbors(key, k);
    }

    @Override
    public PhKnnQuery<V> nearestNeighbour(int i, PhDistance phDistance, PhFilter phDimFilter, long... keys) {
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
    public <R> List<R> queryAll(long[] min, long[] max, int maxResults, PhFilter filter, PhMapper<V, R> mapper) {
        return proxy.queryAll(min, max, maxResults, filter, mapper);
    }

    public PHTreeIndexProxy<V> getProxy() {
        return proxy;
    }

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public PhRangeQuery<V> rangeQuery(double dist, long... center) {
		throw new UnsupportedOperationException();
	}

	@Override
	public PhRangeQuery<V> rangeQuery(double dist, PhDistance optionalDist, long... center) {
		throw new UnsupportedOperationException();
	}
}