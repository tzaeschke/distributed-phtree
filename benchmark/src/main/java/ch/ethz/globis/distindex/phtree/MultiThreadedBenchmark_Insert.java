package ch.ethz.globis.distindex.phtree;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Threads(4)
public class MultiThreadedBenchmark_Insert extends ConcurrencyBenchmarkBase {

    @Benchmark
    public Object putRandom_COW(ConcurrencyBenchmarkBase.BenchmarkState state) {
        state.setCopyOnWrite();

        return put(state);
    }

    @Benchmark
    public Object putRandom_HandOverHand(ConcurrencyBenchmarkBase.BenchmarkState state) {
        state.setHandOverHandLocking();

        return put(state);
    }

    @Benchmark
    public Object putRandom_OL(ConcurrencyBenchmarkBase.BenchmarkState state) {
        state.setOptimisticLocking();

        return put(state);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + MultiThreadedBenchmark_Insert.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
