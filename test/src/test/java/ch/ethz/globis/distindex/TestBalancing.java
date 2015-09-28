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

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.middleware.PhTreeRequestHandler;
import ch.ethz.globis.distindex.test.BaseParameterizedTest;
import org.junit.*;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class TestBalancing extends BaseParameterizedTest {

    private static final Logger LOG = LoggerFactory.getLogger(TestBalancing.class);

    private PHTreeIndexProxy<String> phTree;

    public TestBalancing(int nrServers) throws IOException {
        super(nrServers, true);
    }

    @BeforeClass
    public static void changeBalancingParameters() {
        PhTreeRequestHandler.THRESHOLD = 100;
    }

    @AfterClass
    public static void restoredBalancingParameters() {
        PhTreeRequestHandler.THRESHOLD = Integer.MAX_VALUE;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{5}});
    }

    @Before
    public void setupTree() {
        phTree = new PHTreeIndexProxy<>(HOST, ZK_PORT);
    }

    @After
    public void closeTree() throws IOException {
        phTree.close();
    }

    @Test
    public void insertSameHost_AllPositive_1D() throws InterruptedException {
        phTree.create(1, 64);

        int size = 101;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        for (int i = 0; i < size; i++) {
            long[] key = {i};
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }
        LOG.info("Done inserting {} randomly generated entries.", size);

        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }

    @Test
    public void insertSameHost_AllPositive_2D() throws InterruptedException {
        phTree.create(2, 64);

        int size = 101;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        for (int i = 0; i < size; i++) {
            long[] key = {i, i};
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }
        LOG.info("Done inserting {} randomly generated entries.", size);

        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }

    @Test
    public void insertSameHost_AllPositive_Multiple() throws InterruptedException {
        phTree.create(2, 64);

        int size = 201;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        for (int i = 1; i <= size; i++) {
            long[] key = {i, i};
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }

        LOG.info("Done inserting {} randomly generated entries.", size);
        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }

    @Test
    public void insertSameHost_AllNegative() throws InterruptedException {
        phTree.create(2, 64);
        int size = 101;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        for (int i = 1; i <= size; i++) {
            long[] key = {-i, -i};
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }
        LOG.info("Done inserting {} randomly generated entries.", size);
        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }

    @Test
    public void insertSameHost_AllNegative_Multiple() throws InterruptedException {
        phTree.create(2, 64);

        int size = 201;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        for (int i = 1; i <= size; i++) {
            long[] key = {-i, -i};
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }
        LOG.info("Done inserting {} randomly generated entries.", size);
        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }

    @Test
    public void insertHashedWithinCluster() throws InterruptedException {
        phTree.create(2, 64);

        int size = 300;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        long[][] signs = {{1L, 1L}, {1L, -1L}, {-1L, 1L}, {-1L, -1L}};
        for (int i = 0; i < size; i++) {
            long[] key = {i, i};
            for (int j = 0; j < key.length; j++) {
                key[j] = key[j] * signs[i % key.length][j];
            }
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }
        LOG.info("Done inserting {} randomly generated entries.", size);
        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }
}