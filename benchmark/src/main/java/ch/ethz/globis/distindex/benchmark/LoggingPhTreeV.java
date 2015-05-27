package ch.ethz.globis.distindex.benchmark;

import ch.ethz.globis.pht.*;
import ch.ethz.globis.pht.util.PhMapper;
import ch.ethz.globis.pht.util.PhTreeQStats;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    public int getNodeCount() {
        return tree.getNodeCount();
    }

    @Override
    public PhTreeQStats getQuality() {
        return tree.getQuality();
    }

    @Override
    public PhTreeHelper.Stats getStats() {
        return tree.getStats();
    }

    @Override
    public PhTreeHelper.Stats getStatsIdealNoNode() {
        return tree.getStatsIdealNoNode();
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
    public PhIterator<T> queryExtent() {
        return tree.queryExtent();
    }

    @Override
    public PhIterator<T> query(long[] min, long[] max) {
        return tree.query(min, max);
    }

    @Override
    public int getDIM() {
        return tree.getDIM();
    }

    @Override
    public int getDEPTH() {
        return tree.getDEPTH();
    }

    @Override
    public List<long[]> nearestNeighbour(int nMin, long... key) {
        return tree.nearestNeighbour(nMin, key);
    }

    @Override
    public List<long[]> nearestNeighbour(int nMin, PhDistance dist, PhDimFilter dims, long... key) {
        return tree.nearestNeighbour(nMin, dist, dims, key);
    }

    @Override
    public List<PhEntry<T>> queryAll(long[] min, long[] max) {
        return tree.queryAll(min, max);
    }

    @Override
    public <R> List<R> queryAll(long[] min, long[] max, int maxResults, PhPredicate filter, PhMapper<T, R> mapper) {
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
}