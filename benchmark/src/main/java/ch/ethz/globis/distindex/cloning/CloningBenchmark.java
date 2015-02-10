package ch.ethz.globis.distindex.cloning;

import com.esotericsoftware.kryo.Kryo;
import com.rits.cloning.Cloner;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.zoodb.index.critbit.CritBit64;

import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * The cloning of large objects (CritBit64) is required for cloning nodes during the COW
 * operations. This benchmarks aims to determine the fastest way to obtain deep clones
 * of arbitrary objects, without having to do so manually.
 *
 */
@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.NANOSECONDS)
@Measurement(iterations = 20, time = 100, timeUnit = TimeUnit.NANOSECONDS)
public class CloningBenchmark {

    @Benchmark
    public Object deepClone_Cloning(BenchmarkState state) {
        Cloner cloner = new Cloner();
        return cloner.deepClone(state.index);
    }

    @Benchmark
    public Object deepClone_Kryo(BenchmarkState state) {
        Kryo kryo = state.kryos.get();

        return kryo.copy(state.index);
    }

    @Benchmark
    public Object deepClone_Manual(BenchmarkState state) {
        return state.manualClone(state.index);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + CloningBenchmark.class.getSimpleName() + ".*")
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        private CritBit64 index;

        private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
            protected Kryo initialValue() {
                Kryo kryo = new Kryo();
                ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).
                        setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
                return kryo;
            };
        };

        @Setup
        public void init() {
            index = CritBit64.create();
            Random r = new Random();
            for (int i = 0; i < 1000; i++) {
                index.put(r.nextLong(), new BigInteger(64, r));
            }
        }

        public CritBit64 manualClone(CritBit64 index) {
            CritBit64 copy = CritBit64.create();
            CritBit64.CBIterator it = index.iterator();
            CritBit64.Entry e;
            while (it.hasNext()) {
                e = it.nextEntry();
                copy.put(e.key(), e.value());
            }
            return copy;
        }
    }
}

