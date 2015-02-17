package ch.ethz.globis.distindex.phtree;


import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.PhTreeHelper;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 15, time = 10, timeUnit = TimeUnit.MILLISECONDS)
public class ConcurrencyBenchmark {

    @Benchmark
    public Object deleteRandom_noConcurrent(BenchmarkState state) {
        PhTreeHelper.CONCURRENT = false;

        return delete(state);
    }

    @Benchmark
    public Object deleteRandom_COW(BenchmarkState state) {
        PhTreeHelper.CONCURRENT = true;

        return delete(state);
    }

    @Benchmark
    public Object deleteRandom_BigLock(BenchmarkState state) {
        PhTreeHelper.CONCURRENT = false;
        try {
            state.l.lock();
            return delete(state);
        } finally {
            state.l.unlock();
        }
    }

    private boolean delete(BenchmarkState state) {
        PhTree tree = state.getTree();

        int dim = tree.getDIM();

        long[] key = createRandomKey(dim);
        return state.tree.delete(key);
    }

    @Benchmark
    public Object containsRandom_noConcurrent(BenchmarkState state) {
        PhTreeHelper.CONCURRENT = false;

        return contains(state);
    }

    @Benchmark
    public Object containsRandom_COW(BenchmarkState state) {
        PhTreeHelper.CONCURRENT = true;

        return contains(state);
    }

    @Benchmark
    public Object containsRandom_BigLock(BenchmarkState state) {
        PhTreeHelper.CONCURRENT = false;
        try {
            state.l.lock();
            return contains(state);
        } finally {
            state.l.unlock();
        }
    }

    private boolean contains(BenchmarkState state) {
        int dim = state.getTree().getDIM();

        long[] key = createRandomKey(dim);
        return state.getTree().contains(key);
    }

    @Benchmark
    public Object putRandom_NoConcurrent(BenchmarkState state) {
        PhTreeHelper.CONCURRENT = false;

        return put(state);
    }

    @Benchmark
    public Object putRandom_COW(BenchmarkState state) {
        PhTreeHelper.CONCURRENT = true;

        return put(state);
    }

    @Benchmark
    public Object putRandom_BigLock(BenchmarkState state) {
        PhTreeHelper.CONCURRENT = false;
        try {
            state.l.lock();
            return put(state);
        } finally {
            state.l.unlock();
        }
    }

    private Object put(BenchmarkState state) {
        int dim = state.getTree().getDIM();

        long[] key = createRandomKey(dim);
        return state.getTree().insert(key);
    }

    private long[] createRandomKey(int dim) {
        long[] key = new long[dim];
        Random random = new Random();
        for (int i = 0; i < dim; i++) {
            key[i] = random.nextInt();
        }
        return key;
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        PhTree tree;
        ReentrantLock l = new ReentrantLock(true);

        @Setup
        public void initTree() {
            tree = PhTree.create(2, 64);
        }

        public PhTree getTree() {
            return tree;
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + ConcurrencyBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}