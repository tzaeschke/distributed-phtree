package ch.ethz.globis.distindex.benchmark;

import ch.ethz.globis.pht.PhTreeV;

import java.util.List;
import java.util.concurrent.Callable;

public class MultiInsertionBenchmark extends MultiBenchmark {


    public MultiInsertionBenchmark(int nrThreads, PhTreeV<Object> tree, List<long[]> entries) {
        super(nrThreads, tree, entries);
    }

    @Override
    protected Callable<Result> createTask(int i, int nrEntriesPerThread, PhTreeV<Object> tree, List<long[]> entries) {
        return new ThreadedInserter(i * nrEntriesPerThread, (i + 1) * nrEntriesPerThread, tree, entries);
    }
}

