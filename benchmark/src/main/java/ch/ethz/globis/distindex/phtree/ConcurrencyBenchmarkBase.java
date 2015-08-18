package ch.ethz.globis.distindex.phtree;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import ch.ethz.globis.pht.v5.PhOperations;
import ch.ethz.globis.pht.v5.PhOperationsCOW;
import ch.ethz.globis.pht.v5.PhOperationsHandOverHand_COW;
import ch.ethz.globis.pht.v5.PhOperationsOL_COW;
import ch.ethz.globis.pht.v5.PhOperationsSimple;
import ch.ethz.globis.pht.v5.PhTree5;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrencyBenchmarkBase {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        PhTree5<String> tree;
        final Object lock = new Object();
        final ReentrantLock l = new ReentrantLock();

        private PhOperations phOperationsOL;
        private PhOperations phOperationsHoH;
        private PhOperations phOperationsCOW;
        private PhOperations phOperationsSimple;

        @Setup
        public void initTree() {
            tree = new PhTree5<String>(2, 64);
            phOperationsOL = new PhOperationsOL_COW(tree);
            phOperationsCOW = new PhOperationsCOW(tree);
            phOperationsHoH = new PhOperationsHandOverHand_COW(tree);
            phOperationsSimple = new PhOperationsSimple(tree);
        }

        public PhTree5<String> getTree() {
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

    protected Object put(BenchmarkState state) {
        int dim = state.getTree().getDIM();

        long[] key = createRandomKey(dim);
        return state.getTree().put(key, Arrays.toString(key));
    }

    protected boolean contains(BenchmarkState state) {
        int dim = state.getTree().getDIM();

        long[] key = createRandomKey(dim);
        return state.getTree().contains(key);
    }

    protected String delete(BenchmarkState state) {
        PhTree5<String> tree = state.getTree();

        int dim = tree.getDIM();

        long[] key = createRandomKey(dim);
        return state.tree.remove(key);
    }

    protected long[] createRandomKey(int dim) {
        long[] key = new long[dim];
        Random random = new Random();
        for (int i = 0; i < dim; i++) {
            key[i] = random.nextInt();
        }
        return key;
    }
}
