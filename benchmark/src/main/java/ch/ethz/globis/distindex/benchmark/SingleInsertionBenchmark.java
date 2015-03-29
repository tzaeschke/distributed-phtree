package ch.ethz.globis.distindex.benchmark;

import ch.ethz.globis.pht.v5.PhTree5;

import java.util.List;

public class SingleInsertionBenchmark implements Benchmark {

    private final PhTree5<Object> tree;
    private final List<long[]> entries;

    public SingleInsertionBenchmark(PhTree5<Object> tree, List<long[]> entries) {
        this.tree = tree;
        this.entries = entries;
    }

    @Override
    public Result run() {
        long start = System.currentTimeMillis();
        double averageResponseTime = 0;
        long s, e;
        for (long[] entry :  entries) {
            s = System.currentTimeMillis();
            tree.put(entry, null);
            e = System.currentTimeMillis();
            averageResponseTime += e - s;
        }

        long end = System.currentTimeMillis();
        int nrEntries = entries.size();
        averageResponseTime /= nrEntries;
        return new Result(start, end, nrEntries, averageResponseTime);
    }
}
