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
package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.middleware.PhTreeRequestHandler;
import ch.ethz.globis.distindex.test.BaseParameterizedTest;
import org.junit.*;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TestConcurrent extends BaseParameterizedTest {

    private static final int NR_CLIENTS = 2;

    private PHTreeIndexProxy<Integer>[] phTrees = new PHTreeIndexProxy[NR_CLIENTS];

    public TestConcurrent(int nrServers) throws IOException {
        super(nrServers, true);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{2}, {3}, {4}});
    }

    @Before
    public void setupTree() {
        for (int i = 0; i < NR_CLIENTS; i++) {
            phTrees[i] = new PHTreeIndexProxy<>(HOST, ZK_PORT);
        }
    }

    @After
    public void closeTree() throws IOException {
        for (int i = 0; i < NR_CLIENTS; i++) {
            phTrees[i].close();
        }
    }

    @BeforeClass
    public static void changeBalancingParameters() {
        PhTreeRequestHandler.THRESHOLD = 100;
    }

    @AfterClass
    public static void restoredBalancingParameters() {
        PhTreeRequestHandler.THRESHOLD = Integer.MAX_VALUE;
    }

    @Test
    public void testConcurrentInserts() {
        int dim = 3;
        int depth = 64;
        phTrees[0].create(dim, depth);
        phTrees[1].create(dim, depth);
        ExecutorService pool = Executors.newFixedThreadPool(NR_CLIENTS);

        int entries = 103;
        long[][] keyPositive = new long[entries][dim], keysNegative = new long[entries][dim];
        for (int i = 1; i < entries; i++) {
            for (int j = 0; j < dim; j++) {
                keyPositive[i][j] = i;
                keysNegative[i][j] = -i;
            }
        }
        ThreadedInserter pos = new ThreadedInserter(keyPositive, phTrees[0]);
        ThreadedInserter neg = new ThreadedInserter(keysNegative, phTrees[1]);

        pool.execute(pos);
        pool.execute(neg);
        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (long[] key : keyPositive) {
            assertTrue(phTrees[0].contains(key));
            assertTrue(phTrees[1].contains(key));
        }
        for (long[] key : keysNegative) {
            assertTrue(phTrees[0].contains(key));
            assertTrue(phTrees[1].contains(key));
        }
    }

    @Test
    public void testConcurrentInserts_SameHost() {
        int dim = 3;
        int depth = 64;
        phTrees[0].create(dim, depth);
        phTrees[1].create(dim, depth);
        ExecutorService pool = Executors.newFixedThreadPool(NR_CLIENTS);

        int entries = 153;
        long[][] keysFirst = new long[entries][dim], keysSecond = new long[entries][dim];
        for (int i = 1; i < entries; i++) {
            for (int j = 0; j < dim; j++) {
                keysFirst[i][j] = i;
                keysSecond[i][j] = i + entries;
            }
        }
        ThreadedInserter pos = new ThreadedInserter(keysFirst, phTrees[0]);
        ThreadedInserter neg = new ThreadedInserter(keysSecond, phTrees[1]);

        pool.execute(pos);
        pool.execute(neg);
        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (long[] key : keysFirst) {
            assertTrue(phTrees[0].contains(key));
            assertTrue(phTrees[1].contains(key));
        }
        for (long[] key : keysSecond) {
            assertTrue(phTrees[0].contains(key));
            assertTrue(phTrees[1].contains(key));
        }
    }

    class ThreadedInserter implements Runnable {

        long[][] keys;
        PHTreeIndexProxy<Integer> phTree;

        ThreadedInserter(long[][] keys, PHTreeIndexProxy<Integer> phTree) {
            this.keys = keys;
            this.phTree = phTree;
        }

        @Override
        public void run() {
            for (long[] key: keys) {
                phTree.put(key, null);
            }
        }
    }

}
