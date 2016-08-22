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
package ch.ethz.globis.distindex.concurrency.dummies;

import java.util.List;

import ch.ethz.globis.phtree.PhDistance;
import ch.ethz.globis.phtree.PhEntry;
import ch.ethz.globis.phtree.PhFilter;
import ch.ethz.globis.phtree.PhRangeQuery;
import ch.ethz.globis.phtree.PhTree;
import ch.ethz.globis.phtree.util.PhMapper;
import ch.ethz.globis.phtree.util.PhTreeStats;

/**
 * A concurrent version of the PHTree
 * 
 * NOTE: This is not a proper concurrent version! As a VERY BASIC workaround, all methods
 * are 'synchronized'. But of course this would also require synchronization of iterators from
 * queries. 
 * 
 * @author ztilmann
 *
 */
public class PhTreeC<T> implements PhTree<T> {

	private final PhTree<T> p;
	
	private PhTreeC(int dims) {
		p = PhTree.create(dims);
	}
	
	public static <T> PhTreeC<T> create(int dims) {
		return new PhTreeC<>(dims);
	}
	
	
	@Override
	public synchronized int size() {
		return p.size();
	}

	@Override
	public synchronized PhTreeStats getStats() {
		return p.getStats();
	}

	@Override
	public synchronized T put(long[] key, T value) {
		return p.put(key, value);
	}

	@Override
	public synchronized boolean contains(long... key) {
		return p.contains(key);
	}

	@Override
	public synchronized T get(long... key) {
		return p.get(key);
	}

	@Override
	public synchronized T remove(long... key) {
		return p.remove(key);
	}

	@Override
	public synchronized String toStringPlain() {
		return p.toStringPlain();
	}

	@Override
	public synchronized String toStringTree() {
		return p.toStringTree();
	}

	@Override
	public synchronized PhExtent<T> queryExtent() {
		return p.queryExtent();
	}

	@Override
	public synchronized PhQuery<T> query(long[] min, long[] max) {
		return p.query(min, max);
	}

	@Override
	public synchronized int getDim() {
		return p.getDim();
	}

	@Override
	public synchronized int getBitDepth() {
		return p.getBitDepth();
	}

	@Override
	public synchronized PhKnnQuery<T> nearestNeighbour(int nMin, long... key) {
		return p.nearestNeighbour(nMin, key);
	}

	@Override
	public synchronized PhKnnQuery<T> nearestNeighbour(int nMin, PhDistance dist, PhFilter dims,
			long... key) {
		return p.nearestNeighbour(nMin, dist, dims, key);
	}

	@Override
	public synchronized PhRangeQuery<T> rangeQuery(double dist, long... center) {
		return p.rangeQuery(dist, center);
	}

	@Override
	public synchronized PhRangeQuery<T> rangeQuery(double dist, PhDistance optionalDist, long... center) {
		return p.rangeQuery(dist, optionalDist, center);
	}

	@Override
	public synchronized T update(long[] oldKey, long[] newKey) {
		return p.update(oldKey, newKey);
	}

	@Override
	public synchronized List<PhEntry<T>> queryAll(long[] min, long[] max) {
		return p.queryAll(min, max);
	}

	@Override
	public synchronized <R> List<R> queryAll(long[] min, long[] max, int maxResults, 
			PhFilter filter, PhMapper<T, R> mapper) {
		return p.queryAll(min, max, maxResults, filter, mapper);
	}

	@Override
	public synchronized void clear() {
		p.clear();
	}

	public synchronized void setOperations(PhOperations<T> ops) {
		if (ops instanceof PhOperationsSimple) {
			System.err.println("WARNING: Setting operation for the PhTree is deprecated.");
		} else {
			throw new UnsupportedOperationException();
		}
	}

}
