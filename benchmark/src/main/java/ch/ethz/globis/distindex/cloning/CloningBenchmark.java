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

        private CritBit64<BigInteger> index;

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

        public CritBit64<BigInteger> manualClone(CritBit64<BigInteger> index) {
            CritBit64<BigInteger> copy = CritBit64.create();
            CritBit64.CBIterator<BigInteger> it = index.iterator();
            CritBit64.Entry<BigInteger> e;
            while (it.hasNext()) {
                e = it.nextEntry();
                copy.put(e.key(), e.value());
            }
            return copy;
        }
    }
}

