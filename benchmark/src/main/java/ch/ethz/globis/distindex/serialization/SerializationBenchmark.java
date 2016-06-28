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
package ch.ethz.globis.distindex.serialization;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import ch.ethz.globis.distindex.serializer.FullTreeSerializer;
import ch.ethz.globis.distindex.serializer.IterativeSerializer;
import ch.ethz.globis.pht.PhTree;

@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 15, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class SerializationBenchmark {

    private static final String FULL_TREE_FILE = "full_tree.txt";
    private static final String ITERATIVE_TREE_FILE = "iterative_tree.txt";

    @Benchmark
    public PhTree<String> serialize_FullTree(BenchmarkState state) throws FileNotFoundException {
        FullTreeSerializer serializer = new FullTreeSerializer();

        serializer.export(state.tree, FULL_TREE_FILE);

        return state.tree;
    }

    @Benchmark
    public PhTree<String> serialize_Iterative(BenchmarkState state) throws FileNotFoundException {
        IterativeSerializer<String> serializer = new IterativeSerializer<String>();

        serializer.export(state.tree, ITERATIVE_TREE_FILE);

        return state.tree;
    }

    @Benchmark
    public PhTree<String> deserialize_FullTree() throws FileNotFoundException {
        FullTreeSerializer serializer = new FullTreeSerializer();
        return serializer.load(FULL_TREE_FILE);
    }

    @Benchmark
    public PhTree<String> deserialize_IterativeTree(BenchmarkState state) throws FileNotFoundException {
        IterativeSerializer<String> serializer = new IterativeSerializer<String>();

        serializer.setTree(PhTree.create(state.dim));
        return serializer.load(ITERATIVE_TREE_FILE);
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private PhTree<String> tree;
        int dim = 6;
        int depth = 64;

        @Setup
        public void init() throws FileNotFoundException {

            //create a tree containing random entries
            int nrEntries = 10000;
            tree = PhTree.create(dim);

            long[] key = new long[dim];
            Random random = new Random();

            for (int i = 0; i < nrEntries; i++) {
                for (int j = 0; j < dim; j++) {
                    key[j] = random.nextInt();
                }
                tree.put(key, Arrays.toString(key));
            }

            //serialize the tree so that de-serialization benchmark works every time
            FullTreeSerializer serializer = new FullTreeSerializer();
            serializer.export(tree, FULL_TREE_FILE);

            IterativeSerializer<String> itSerializer = new IterativeSerializer<String>();
            itSerializer.export(tree, ITERATIVE_TREE_FILE);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + SerializationBenchmark.class.getSimpleName() + ".*")
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}