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
package ch.ethz.globis.distindex.benchmark;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import ch.ethz.globis.phtree.PhDistance;
import ch.ethz.globis.phtree.PhEntry;
import ch.ethz.globis.phtree.PhFilter;
import ch.ethz.globis.phtree.PhRangeQuery;
import ch.ethz.globis.phtree.PhTree;
import ch.ethz.globis.phtree.util.PhMapper;
import ch.ethz.globis.phtree.util.PhTreeStats;

/**
 * Performs an in-memory logging
 * @param <T>
 */
public class LoggingPhTreeV<T> implements PhTree<T> {

    /** Concurrent linked-list containing the log messages. */
    private ConcurrentLinkedQueue<String> log = new ConcurrentLinkedQueue<>();

    /** The tree on which we're doing the operations. */
    private PhTree<T> tree;

    private static enum Operation {
        PUT, GET, CONTAINS, DELETE, UPDATE
    }

    public LoggingPhTreeV(PhTree<T> tree) {
        this.tree = tree;
    }

    @Override
    public int size() {
        return tree.size();
    }

    @Override
    public PhTreeStats getStats() {
        return tree.getStats();
    }

    @Override
    public T put(long[] key, T value) {
        try {
            logStart(Operation.PUT);
            return tree.put(key, value);
        } finally {
            logEnd(Operation.PUT);
        }
    }

    @Override
    public boolean contains(long... key) {
        try {
            logStart(Operation.CONTAINS);
            return tree.contains(key);
        } finally {
            logEnd(Operation.CONTAINS);
        }

    }

    @Override
    public T get(long... key) {
        try {
            logStart(Operation.GET);
            return tree.get(key);
        } finally {
            logEnd(Operation.GET);
        }
    }

    @Override
    public T remove(long... key) {
        try {
            logStart(Operation.DELETE);
            return tree.remove(key);
        } finally {
            logEnd(Operation.DELETE);
        }
    }

    @Override
    public T update(long[] oldKey, long[] newKey) {
        try {
            logStart(Operation.UPDATE);
            return tree.update(oldKey, newKey);
        } finally {
            logEnd(Operation.UPDATE);
        }
    }

    @Override
    public String toStringPlain() {
        return tree.toStringPlain();
    }

    @Override
    public String toStringTree() {
        return tree.toStringTree();
    }

    @Override
    public PhExtent<T> queryExtent() {
        return tree.queryExtent();
    }

    @Override
    public PhQuery<T> query(long[] min, long[] max) {
        return tree.query(min, max);
    }

    @Override
    public int getDim() {
        return tree.getDim();
    }

    @Override
    public int getBitDepth() {
        return tree.getBitDepth();
    }

    @Override
    public PhKnnQuery<T> nearestNeighbour(int nMin, long... key) {
        return tree.nearestNeighbour(nMin, key);
    }

    @Override
    public PhKnnQuery<T> nearestNeighbour(int nMin, PhDistance dist, PhFilter dims, long... key) {
        return tree.nearestNeighbour(nMin, dist, dims, key);
    }

    @Override
    public List<PhEntry<T>> queryAll(long[] min, long[] max) {
        return tree.queryAll(min, max);
    }

    @Override
    public <R> List<R> queryAll(long[] min, long[] max, int maxResults, PhFilter filter, PhMapper<T, R> mapper) {
        return tree.queryAll(min, max, maxResults, filter, mapper);
    }

    private void logStart(Operation operation) {
        log.add(operation + " started at " + System.currentTimeMillis());
    }

    private void logEnd(Operation operation) {
        log.add(operation + " finished at " + System.currentTimeMillis());
    }

    public void getLogStats(String filename) {

        try (BufferedWriter br = new BufferedWriter(new FileWriter(filename))) {
            String s;
            while (log.isEmpty()) {
                s = log.poll();
                br.write(s);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void computeLogStats() {
        TreeMap<Long, Integer> map = new TreeMap<>();
        String s;
        String[] splits;
        Integer count;
        Long time;
        while (log.isEmpty()) {
            s = log.poll();
            if (s.matches(" finished at ")) {
                splits = s.split(" finished at ");
                time = getSecond(splits[1]);
                count = map.get(time) + 1;
                map.put(time, count);
            }
        }
        System.out.println(map);
    }

    private Long getSecond(String timeString) {
        Long l = Long.parseLong(timeString);
        return l / 1000;
    }

	@Override
	public void clear() {
		tree.clear();
	}

	@Override
	public PhRangeQuery<T> rangeQuery(double dist, long... center) {
		return tree.rangeQuery(dist, center);
	}

	@Override
	public PhRangeQuery<T> rangeQuery(double dist, PhDistance optionalDist, long... center) {
		return tree.rangeQuery(dist, optionalDist, center);
	}
}