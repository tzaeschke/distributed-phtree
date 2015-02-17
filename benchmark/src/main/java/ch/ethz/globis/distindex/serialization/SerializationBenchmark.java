package ch.ethz.globis.distindex.serialization;

import ch.ethz.globis.distindex.serializer.FullTreeSerializer;
import ch.ethz.globis.distindex.serializer.IterativeSerializer;
import ch.ethz.globis.pht.PhTreeV;
import ch.ethz.globis.pht.v5.PhTree5;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 15, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class SerializationBenchmark {

    private static final String FULL_TREE_FILE = "full_tree.txt";
    private static final String ITERATIVE_TREE_FILE = "iterative_tree.txt";

    @Benchmark
    public PhTreeV<String> serialize_FullTree(BenchmarkState state) throws FileNotFoundException {
        FullTreeSerializer serializer = new FullTreeSerializer();

        serializer.export(state.tree, FULL_TREE_FILE);

        return state.tree;
    }

    @Benchmark
    public PhTreeV<String> serialize_Iterative(BenchmarkState state) throws FileNotFoundException {
        IterativeSerializer<String> serializer = new IterativeSerializer<String>();

        serializer.export(state.tree, ITERATIVE_TREE_FILE);

        return state.tree;
    }

    @Benchmark
    public PhTreeV<String> deserialize_FullTree() throws FileNotFoundException {
        FullTreeSerializer serializer = new FullTreeSerializer();
        return serializer.load(FULL_TREE_FILE);
    }

    @Benchmark
    public PhTreeV<String> deserialize_IterativeTree(BenchmarkState state) throws FileNotFoundException {
        IterativeSerializer<String> serializer = new IterativeSerializer<String>();

        serializer.setTree(new PhTree5<String>(state.dim, state.depth));
        return serializer.load(ITERATIVE_TREE_FILE);
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private PhTreeV<String> tree;
        int dim = 6;
        int depth = 64;

        @Setup
        public void init() throws FileNotFoundException {

            //create a tree containing random entries
            int nrEntries = 100000;
            tree = new PhTree5<String>(dim, depth);

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