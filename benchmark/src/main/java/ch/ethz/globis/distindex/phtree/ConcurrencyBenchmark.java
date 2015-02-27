package ch.ethz.globis.distindex.phtree;


import ch.ethz.globis.pht.v5.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Threads(2)
public class ConcurrencyBenchmark {

//    @Benchmark
//    public Object deleteRandom_noConcurrent(BenchmarkState state) {
//        state.setNoConcurrency();
//
//        return delete(state);
//    }
//
//    @Benchmark
//    public Object deleteRandom_COW(BenchmarkState state) {
//        state.setCopyOnWrite();
//
//        return delete(state);
//    }
//
//    @Benchmark
//    public Object deleteRandom_HandOverHand(BenchmarkState state) {
//        state.setHandOverHandLocking();
//
//        return delete(state);
//    }
//
//    @Benchmark
//    public Object deleteRandom_OL(BenchmarkState state) {
//        state.setOptimisticLocking();
//
//        return delete(state);
//    }
//
//    @Benchmark
//    public Object deleteRandom_BigLock(BenchmarkState state) {
//        state.setNoConcurrency();
//        try {
//            state.l.lock();
//            return delete(state);
//        } finally {
//            state.l.unlock();
//        }
//    }

    private String delete(BenchmarkState state) {
        PhTree5<String> tree = state.getTree();

        int dim = tree.getDIM();

        long[] key = createRandomKey(dim);
        return state.tree.remove(key);
    }

//    @Benchmark
//    public Object containsRandom_noConcurrent(BenchmarkState state) {
//        state.setNoConcurrency();
//
//        return contains(state);
//    }
//
//    @Benchmark
//    public Object containsRandom_COW(BenchmarkState state) {
//        state.setCopyOnWrite();
//
//        return contains(state);
//    }
//
//    @Benchmark
//    public Object containsRandom_HandOverHand(BenchmarkState state) {
//        state.setHandOverHandLocking();
//
//        return contains(state);
//    }
//
//    @Benchmark
//    public Object containsRandom_OL(BenchmarkState state) {
//        state.setOptimisticLocking();
//
//        return contains(state);
//    }
//
//    @Benchmark
//    public Object containsRandom_BigLock(BenchmarkState state) {
//        state.setNoConcurrency();
//        synchronized (state.lock) {
//            return contains(state);
//        }
//    }

    private boolean contains(BenchmarkState state) {
        int dim = state.getTree().getDIM();

        long[] key = createRandomKey(dim);
        return state.getTree().contains(key);
    }

    @Benchmark
    public Object putRandom_NoConcurrent(BenchmarkState state) {
        state.setNoConcurrency();

        return put(state);
    }

    @Benchmark
    public Object putRandom_COW(BenchmarkState state) {
        state.setCopyOnWrite();

        return put(state);
    }

    @Benchmark
    public Object putRandom_HandOverHand(BenchmarkState state) {
        state.setHandOverHandLocking();

        return put(state);
    }

    @Benchmark
    public Object putRandom_OL(BenchmarkState state) {
        state.setOptimisticLocking();

        return put(state);
    }

    @Benchmark
    public Object putRandom_BigLock(BenchmarkState state) {
        state.setNoConcurrency();

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
        return state.getTree().put(key, Arrays.toString(key));
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

        PhTree5<String> tree;
        final Object lock = new Object();
        final ReentrantLock l = new ReentrantLock();

        private PhOperations phOperationsOL;
        private PhOperations phOperationsHoH;
        private PhOperations phOperationsCOW;
        private PhOperations phOperationsSimple;

        @Setup
        public void initTree() {
            tree = new PhTree5<String>(2, 64);
            phOperationsOL = new PhOperationsOL_COW(tree);
            phOperationsCOW = new PhOperationsCOW(tree);
            phOperationsHoH = new PhOperationsHandOverHand_COW(tree);
            phOperationsSimple = new PhOperationsSimple(tree);
        }

        public PhTree5<String> getTree() {
            return tree;
        }

        public void setOptimisticLocking() {
            tree.setOperations(phOperationsOL);
        }

        public void setHandOverHandLocking() {
            tree.setOperations(phOperationsHoH);
        }

        public void setCopyOnWrite() {
            tree.setOperations(phOperationsCOW);
        }

        public void setNoConcurrency() {
            tree.setOperations(phOperationsSimple);
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