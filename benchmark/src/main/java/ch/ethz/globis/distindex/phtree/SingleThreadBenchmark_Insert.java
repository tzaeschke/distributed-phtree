package ch.ethz.globis.distindex.phtree;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Threads(1)
public class SingleThreadBenchmark_Insert extends ConcurrencyBenchmarkBase {

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

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + SingleThreadBenchmark_Insert.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
