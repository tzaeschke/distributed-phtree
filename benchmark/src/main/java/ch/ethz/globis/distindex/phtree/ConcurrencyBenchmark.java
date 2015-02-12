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

@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 15, time = 100, timeUnit = TimeUnit.MILLISECONDS)
public class ConcurrencyBenchmark {

    @Benchmark
    public Object putRandom_NoConcurrent(BenchmarkState state) {
        PhTreeHelper.CONCURRENT = false;

        int dim = state.getTree().getDIM();

        long[] key = createRandomKey(dim);
        return state.getTree().insert(key);
    }

    @Benchmark
    public Object putRandom_Concurrent(BenchmarkState state) {
        PhTreeHelper.CONCURRENT = true;

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
                .include(".*" + ConcurrencyBenchmark.class.getSimpleName() + ".*")
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}