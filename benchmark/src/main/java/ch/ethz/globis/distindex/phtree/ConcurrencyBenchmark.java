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
package ch.ethz.globis.distindex.phtree;


import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import ch.ethz.globis.distindex.concurrency.dummies.PhOperations;
import ch.ethz.globis.distindex.concurrency.dummies.PhOperationsCOW;
import ch.ethz.globis.distindex.concurrency.dummies.PhOperationsHandOverHand_COW;
import ch.ethz.globis.distindex.concurrency.dummies.PhOperationsOL_COW;
import ch.ethz.globis.distindex.concurrency.dummies.PhOperationsSimple;
import ch.ethz.globis.distindex.concurrency.dummies.PhTreeC;
import ch.ethz.globis.phtree.PhTree;

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
        PhTree<String> tree = state.getTree();

        int dim = tree.getDim();

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
        int dim = state.getTree().getDim();

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
        int dim = state.getTree().getDim();

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

        PhTreeC<String> tree;
        final Object lock = new Object();
        final ReentrantLock l = new ReentrantLock();

        private PhOperations phOperationsOL;
        private PhOperations phOperationsHoH;
        private PhOperations phOperationsCOW;
        private PhOperations phOperationsSimple;

        @Setup
        public void initTree() {
            tree = PhTreeC.create(2);
            phOperationsOL = new PhOperationsOL_COW(tree);
            phOperationsCOW = new PhOperationsCOW(tree);
            phOperationsHoH = new PhOperationsHandOverHand_COW(tree);
            phOperationsSimple = new PhOperationsSimple(tree);
        }

        public PhTree<String> getTree() {
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