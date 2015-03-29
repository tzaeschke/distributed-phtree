package ch.ethz.globis.distindex.benchmark;

import ch.ethz.globis.pht.v5.PhOperationsCOW;
import ch.ethz.globis.pht.v5.PhOperationsHandOverHand_COW;
import ch.ethz.globis.pht.v5.PhOperationsOL_COW;
import ch.ethz.globis.pht.v5.PhTree5;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class InsertionBenchmark {

    public List<long[]> entries;

    public static List<long[]> generateRandomEntries(int nrEntries, int dim) {
        List<long[]> entries = new ArrayList<>();
        Random random = new Random(42);
        long[] key;
        for (int i = 0; i < nrEntries; i++) {
            key = new long[dim];
            for (int j = 0; j < dim; j++) {
                key[j] = random.nextLong();
            }
            entries.add(key);
        }
        return entries;
    }

    public static List<long[]> generateConsecutiveEntries(int nrEntries, int dim) {
        List<long[]> entries = new ArrayList<>();
        long[] key;
        for (int i = 0; i < nrEntries; i++) {
            key = new long[dim];
            for (int j = 0; j < dim; j++) {
                key[j] = i;
            }
            entries.add(key);
        }
        return entries;
    }

    public static void performMeasurement(PhTree5<Object> index, List<long[]> entries) {
        long start = System.currentTimeMillis();

        for (long[] entry :  entries) {
            index.put(entry, null);
        }
        long end = System.currentTimeMillis();

        long duration = end - start;
        double durationInSeconds = duration / 1000.0;
        int nrEntries = entries.size();

        System.out.println("Inserted " + nrEntries + " entries in " + durationInSeconds + " seconds." );
    }

    private static Result benchmarkNoConcurrent(List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        Benchmark benchmark = new SingleInsertionBenchmark(tree, entries);
        return benchmark.run();
    }

    private static Result benchmarkCOW(List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        tree.setOperations(new PhOperationsCOW<>(tree));
        Benchmark benchmark = new SingleInsertionBenchmark(tree, entries);
        return benchmark.run();
    }

    private static Result benchmarkCOWMulti(int nrThreads, List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        tree.setOperations(new PhOperationsCOW<>(tree));
        Benchmark benchmark = new MultiInsertionBenchmark(nrThreads, tree, entries);
        return benchmark.run();
    }

    private static Result benchmarkCOWMultiRead(int nrThreads, List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        tree.setOperations(new PhOperationsCOW<>(tree));
        Benchmark benchmark = new MultiReadBenchmark(nrThreads, tree, entries);
        return benchmark.run();
    }

    private static Result benchmarkCOWMultiReadWrite(int nrThreads, List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        tree.setOperations(new PhOperationsCOW<>(tree));
        Benchmark benchmark = new MultiInsertReadBenchmark(nrThreads, tree, entries);
        return benchmark.run();
    }

    private static Result benchmarkOL(List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        tree.setOperations(new PhOperationsOL_COW<>(tree));
        Benchmark benchmark = new SingleInsertionBenchmark(tree, entries);
        return benchmark.run();
    }

    private static Result benchmarkOLMulti(int nrThreads, List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        tree.setOperations(new PhOperationsOL_COW<>(tree));
        Benchmark benchmark = new MultiInsertionBenchmark(nrThreads, tree, entries);
        return benchmark.run();
    }

    private static Result benchmarkOLMultiRead(int nrThreads, List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        tree.setOperations(new PhOperationsOL_COW<>(tree));
        Benchmark benchmark = new MultiReadBenchmark(nrThreads, tree, entries);
        return benchmark.run();
    }

    private static Result benchmarkOLMultiReadWrite(int nrThreads, List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        tree.setOperations(new PhOperationsOL_COW<>(tree));
        Benchmark benchmark = new MultiInsertReadBenchmark(nrThreads, tree, entries);
        return benchmark.run();
    }

    private static Result benchmarkHoH(List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        tree.setOperations(new PhOperationsHandOverHand_COW<>(tree));
        Benchmark benchmark = new SingleInsertionBenchmark(tree, entries);
        return benchmark.run();
    }

    private static Result benchmarkHoHMulti(int nrThreads, List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        tree.setOperations(new PhOperationsHandOverHand_COW<>(tree));
        Benchmark benchmark = new MultiInsertionBenchmark(nrThreads, tree, entries);
        return benchmark.run();
    }

    private static Result benchmarkHoHMultiRead(int nrThreads, List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        tree.setOperations(new PhOperationsHandOverHand_COW<>(tree));
        Benchmark benchmark = new MultiReadBenchmark(nrThreads, tree, entries);
        return benchmark.run();
    }

    private static Result benchmarkHoHMultiReadWrite(int nrThreads, List<long[]> entries, int dim, int depth) {
        PhTree5<Object> tree = new PhTree5<>(dim, depth);
        tree.setOperations(new PhOperationsHandOverHand_COW<>(tree));
        Benchmark benchmark = new MultiInsertReadBenchmark(nrThreads, tree, entries);
        return benchmark.run();
    }

    public static void main(String[] args) {

        int depth = 64;
        int nrEntries = 50000;
        int maxThreads = 4;

        if (args.length == 2) {
            nrEntries = Integer.parseInt(args[0]);
            maxThreads = Integer.parseInt(args[1]);
        }
        System.out.println("Nr entries: " + nrEntries);
        System.out.println("Max nr threads: " + maxThreads);
        String line;
        int[] dims = {1, 2, 3, 6, 10};

        int reps = 3;
        System.out.println("*********************************************");
        System.out.println("                Insertion");
        System.out.println("*********************************************");
        for (int dim : dims) {
            System.out.println("Dimensions: " + dim);
            List<long[]> entries = generateRandomEntries(nrEntries, dim);
            List<long[]> testEntries = generateConsecutiveEntries(5000, dim);

            String pattern = "%s\t%30s\t%30s\t%30s";
            System.out.println(String.format(pattern, "Threads", "Copy-on-Write", "Hand-over-Hand", "Optimistic locking"));
            for (int rep = 0; rep < reps; rep++) {
                for (int i = 1; i <= maxThreads; i++) {
                    benchmarkCOWMulti(i, testEntries, dim, depth);
                    benchmarkHoHMulti(i, testEntries, dim, depth);
                    benchmarkOLMulti(i, testEntries, dim, depth);

                    line = String.format(pattern, i, benchmarkCOWMulti(i, entries, dim, depth),
                            benchmarkHoHMulti(i, entries, dim, depth),
                            benchmarkOLMulti(i, entries, dim, depth));

                    System.out.println(line);
                }
            }
        }

        System.out.println("*********************************************");
        System.out.println("                Read");
        System.out.println("*********************************************");
        for (int dim : dims) {
            System.out.println("Dimensions: " + dim);
            List<long[]> entries = generateRandomEntries(nrEntries, dim);
            List<long[]> testEntries = generateConsecutiveEntries(5000, dim);

            String pattern = "%s\t%30s\t%30s\t%30s";
            System.out.println(String.format(pattern, "Threads", "Copy-on-Write", "Hand-over-Hand", "Optimistic locking"));
            for (int rep = 0; rep < reps; rep++) {
                for (int i = 1; i <= maxThreads; i++) {
                    benchmarkCOWMultiRead(i, testEntries, dim, depth);
                    benchmarkHoHMultiRead(i, testEntries, dim, depth);
                    benchmarkOLMultiRead(i, testEntries, dim, depth);

                    line = String.format(pattern, i, benchmarkCOWMultiRead(i, entries, dim, depth),
                            benchmarkHoHMultiRead(i, entries, dim, depth),
                            benchmarkOLMultiRead(i, entries, dim, depth));

                    System.out.println(line);
                }
            }
        }

        System.out.println("*********************************************");
        System.out.println("                Read during inserts");
        System.out.println("*********************************************");
        for (int dim : dims) {
            System.out.println("Dimensions: " + dim);
            List<long[]> entries = generateRandomEntries(nrEntries, dim);
            List<long[]> testEntries = generateConsecutiveEntries(5000, dim);

            String pattern = "%s\t%30s\t%30s\t%30s";
            System.out.println(String.format(pattern, "Threads", "Copy-on-Write", "Hand-over-Hand", "Optimistic locking"));
            for (int rep = 0; rep < reps; rep++) {
                for (int i = 1; i <= maxThreads; i++) {
                    benchmarkCOWMultiReadWrite(i, testEntries, dim, depth);
                    benchmarkHoHMultiReadWrite(i, testEntries, dim, depth);
                    benchmarkOLMultiReadWrite(i, testEntries, dim, depth);

                    line = String.format(pattern, i, benchmarkCOWMultiReadWrite(i, entries, dim, depth),
                            benchmarkHoHMultiReadWrite(i, entries, dim, depth),
                            benchmarkOLMultiReadWrite(i, entries, dim, depth));

                    System.out.println(line);
                }
            }
        }
    }
}