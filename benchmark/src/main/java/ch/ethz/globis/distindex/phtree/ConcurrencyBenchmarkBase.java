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
