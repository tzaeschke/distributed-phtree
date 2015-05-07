package ch.ethz.globis.distindex.benchmark;

import ch.ethz.globis.pht.PhTree;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public abstract class MultiBenchmark implements Benchmark {

    private final int nrThreads;
    private final ExecutorService pool;
    private final PhTree<Object> tree;
    private final List<long[]> entries;

    public MultiBenchmark(int nrThreads, PhTree<Object> tree, List<long[]> entries) {
        this.nrThreads = nrThreads;
        this.tree = tree;
        this.entries = entries;
        this.pool = Executors.newFixedThreadPool(nrThreads);
    }

    @Override
    public Result run() {
        int nrEntries = entries.size();
        int nrEntriesPerThread = nrEntries / nrThreads;

        List<Callable<Result>> tasks = new ArrayList<>();
        for (int i = 0; i < nrThreads; i++) {
            tasks.add(createTask(i, nrEntriesPerThread, tree, entries));
        }

        Result result = null;
        try {
            List<Future<Result>> futures = pool.invokeAll(tasks);

            pool.shutdown();
            pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.NANOSECONDS);

            result = obtainResult(futures);
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("There was an error executing the insertion tasks.");
            e.printStackTrace();
        } finally {
            if (tree instanceof LoggingPhTreeV) ((LoggingPhTreeV) tree).computeLogStats();
        }
        return result;
    }

    protected abstract Callable<Result> createTask(int i, int nrEntriesPerThread, PhTree<Object> tree, List<long[]> entries);

    private Result obtainResult(List<Future<Result>> futures) throws ExecutionException, InterruptedException {
        boolean notFinished;
        do {
            notFinished = false;
            for (Future<Result> future : futures) {
                if (!future.isDone()) {
                    notFinished = true;
                }
            }
        } while (notFinished);

        long nrOperations = 0;
        long  start = Long.MAX_VALUE,
                end = Long.MIN_VALUE;
        double avgResponseTime = 0;

        for (Future<Result> future : futures) {
            Result result = future.get();
            start = Math.min(result.getStart(), start);
            end = Math.max(result.getEnd(), end);
            nrOperations += result.getNrOperations();
            avgResponseTime += result.getAvgResponseTime();
        }
        avgResponseTime /= futures.size() * 1.0;
        return new Result(start, end, nrOperations, avgResponseTime);
    }
}
