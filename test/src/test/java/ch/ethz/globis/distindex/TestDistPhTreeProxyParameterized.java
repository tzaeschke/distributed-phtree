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
import ch.ethz.globis.distindex.api.IndexIterator;
import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.test.BaseParameterizedTest;
import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.pht.*;
import ch.ethz.globis.pht.util.PhMapper;
import ch.ethz.globis.pht.util.PhMapperK;
import ch.ethz.globis.pht.util.PhMapperV;
import ch.ethz.globis.pht.util.PhTreeQStats;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

import static org.junit.Assert.*;

public class TestDistPhTreeProxyParameterized extends BaseParameterizedTest {

    private PHTreeIndexProxy<String> phTree;

    public TestDistPhTreeProxyParameterized(int nrServers) throws IOException {
        super(nrServers, true);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {1},
                {4},
                {7},
                {13}
        });
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
    public void testLargeValues() throws Exception {
        phTree.create(2, 64);

        long[] key = {1L, 2L};
        String veryLargeString = new BigInteger(1024 * 1024, new Random()).toString();

        phTree.put(key, veryLargeString);
        assertEquals(veryLargeString, phTree.get(key));
    }

    @Test
    public void testGet() throws Exception {
        phTree.create(2, 64);
        String retrieved = phTree.get(new long[] { 1L, 2L});

        assertNull("Retrieved value should be null as it is not in the tree.", retrieved);
    }

    @Test
    public void testRangedIterator() throws Exception {
        phTree.create(2, 64);
        IndexEntryList<long[], String> expected = new IndexEntryList<>();
        IndexEntryList<long[], String> toInsert = new IndexEntryList<>();
        toInsert.add(k(0, 1), "foo");
        expected.add(k(0, 1), "foo");
        toInsert.add(k(1, 2), "bar");
        toInsert.add(k(-1, -2), "fuzz");
        toInsert.add(k(-1, -1), "fuz");
        expected.add(k(-1, -1), "fuz");

        for (IndexEntry<long[], String> entry : toInsert) {
            phTree.put(entry.getKey(), entry.getValue());
        }

        Iterator<IndexEntry<long[], String>> it = phTree.query(k(-1, -1), k(1, 1));

        IndexEntryList<long[], String> received = new IndexEntryList<>();
        while (it.hasNext()) {
            IndexEntry<long[], String> entry = it.next();
            received.add(entry);
        }

        assertEquals(expected.size(), received.size());
        for (int i = 0; i < received.size(); i++) {
            assertArrayEquals(expected.get(i).getKey(), received.get(i).getKey());
            assertEquals(expected.get(i).getValue(), received.get(i).getValue());
        }
    }

    @Test
    public void testIterator() throws Exception {
        phTree.create(2, 64);
        IndexEntryList<long[], String> expected = new IndexEntryList<>();

        expected.add(k(0, 0), "foo1");
        expected.add(k(0, 1), "foo");
        expected.add(k(1, 1), "foo");
        expected.add(k(1, 2), "bar");
        expected.add(k(-1, -2), "fuzz");
        expected.add(k(-1, -1), "fuz");

        for (IndexEntry<long[], String> entry : expected) {
            phTree.put(entry.getKey(), entry.getValue());
        }

        IndexIterator<long[], String> it = phTree.iterator();

        IndexEntryList<long[], String> received = new IndexEntryList<>();
        while (it.hasNext()) {
            IndexEntry<long[], String> entry = it.next();
            received.add(entry);
        }

        assertEquals(expected.size(), received.size());
        for (int i = 0; i < received.size(); i++) {
            assertArrayEquals(expected.get(i).getKey(), received.get(i).getKey());
            assertEquals(expected.get(i).getValue(), received.get(i).getValue());
        }
    }

    @Test
    public void testGetRange() throws Exception {
        phTree.create(2, 64);

        IndexEntryList<long[], String> expected = setupTestTreeForRangeQueriesAndReturnExpectedResult();
        IndexEntryList<long[], String> actual = phTree.getRange(new long[]{9, 9}, new long[]{11, 11});

        expected = MultidimUtil.sort(expected);
        actual = MultidimUtil.sort(actual);
        assertEqualsEntryLists(expected, actual);
    }

    @Test
    public void testGetRange2Mid() throws Exception {
        phTree.create(2, 64);

        phTree.put(new long[] {0, 0}, "foo");
        phTree.put(new long[] {1, 0}, "foo");
        phTree.put(new long[]{1, 1}, "foo");
        phTree.put(new long[]{-1, 0}, "foo");
        phTree.put(new long[]{0, 2}, "foo");
        phTree.put(new long[]{-2, 3}, "foo");
        phTree.put(new long[]{-2, 2}, "foo");

        IndexEntryList<long[], String> actual = phTree.getRange(new long[]{-2, -2}, new long[]{2, 2});
        IndexEntryList<long[], String> expected = new IndexEntryList<>();
        expected.add(new IndexEntry<>(new long[] { 0L, 0L}, "foo"));
        expected.add(new IndexEntry<>(new long[] { 1L, 0L}, "foo"));
        expected.add(new IndexEntry<>(new long[] { 1L, 1L}, "foo"));
        expected.add(new IndexEntry<>(new long[] { 0L, 2L}, "foo"));
        expected.add(new IndexEntry<>(new long[] { -1L, 0L}, "foo"));
        expected.add(new IndexEntry<>(new long[] { -2L, 2L}, "foo"));

        expected = MultidimUtil.sort(expected);
        actual = MultidimUtil.sort(actual);
        assertEqualsEntryLists(expected, actual);
    }

    @Test
    public void testPutEmpty() throws Exception {
        phTree.create(2, 64);

        long[] key = {1, 2};
        phTree.put(key, null);

        assertNull(phTree.get(key));
    }

    @Test
    public void testGetKNN() throws Exception {
        phTree.create(2, 64);

        List<long[]> expected = new ArrayList<long[]>() {{
            add(new long[] { 0, 0});
            add(new long[] { 1, 2});
            add(new long[] { -1, -1});
        }};
        List<long[]> inserted = new ArrayList<long[]>();
        inserted.addAll(expected);
        inserted.add(new long[] { -1000, 1000});
        inserted.add(new long[] { -1000, -1000});
        inserted.add(new long[] { 1000, -1000});
        inserted.add(new long[] { 1000, 1000});
        for (long[] key : inserted) {
            phTree.put(key, null);
        }

        List<long[]> nearestNeighbors = 
        		MultidimUtil.knnToList(phTree.getNearestNeighbors(new long[]{0, 0}, 3));

        expected = MultidimUtil.sort(expected);
        nearestNeighbors = MultidimUtil.sort(nearestNeighbors);

        assertEquals(expected.size(), nearestNeighbors.size());
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i), nearestNeighbors.get(i));
        }
    }

    @Test
    public void testPutAndGet2D() throws Exception {
        phTree.create(2, 64);

        long[] key = new long[]{1L, 2L};
        String value = "hello";
        phTree.put(key, value);
        String retrieved = phTree.get(key);
        assertEquals("Wrong value retrieval", value, retrieved);

        key = new long[]{-1L, -2L};
        value = "bye";
        phTree.put(key, value);
        retrieved = phTree.get(key);
        assertEquals("Wrong value retrieval", value, retrieved);
    }

    @Test
    public void testDeleteContains() throws Exception {
        phTree.create(2, 64);

        long[] key1 = {-1, -1};
        long[] key2 = {1, 1};
        String value1 = new BigInteger(30, new Random()).toString();
        String value2 = new BigInteger(30, new Random()).toString();
        phTree.put(key1, value1);
        phTree.put(key2, value2);

        assertTrue(phTree.contains(key1));
        assertTrue(phTree.contains(key2));

        assertEquals(value1, phTree.remove(key1));
        assertFalse(phTree.contains(key1));
        assertTrue(phTree.contains(key2));

        assertEquals(value2, phTree.remove(key2));
        assertFalse(phTree.contains(key1));
        assertFalse(phTree.contains(key2));
    }

    @Test
    public void testPutAndGetRandom2D() throws Exception {
        phTree.create(2, 64);

        int nrEntries = 200;
        Random random = new Random();

        long[] key;
        String value;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();

        for (int i = 0; i < nrEntries; i++) {
            key = new long[]{random.nextLong(), random.nextLong()};
            value = new BigInteger(50, random).toString();
            phTree.put(key, value);
            entries.add(key, value);
        }
        int misses = 0;
        for (IndexEntry<long[], String> entry :  entries) {
            key = entry.getKey();
            value = entry.getValue();
            if (!value.equals(phTree.get(key))) {
                misses++;
            }
        }
        assertEquals("Values that do not match the tree.", 0, misses);
    }

    @Test
    public void testGetRangeMulti() throws Exception {
        phTree.create(2, 64);

        int nrEntries = 200;
        Random random = new Random();

        long[] key;
        String value;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();

        for (int i = 0; i < nrEntries; i++) {
            key = new long[]{random.nextLong(), random.nextLong()};
            value = new BigInteger(50, random).toString();
            phTree.put(key, value);
            entries.add(key, value);
        }

        long[] start = { Long.MIN_VALUE, Long.MIN_VALUE};
        long[] end = {Long.MAX_VALUE, Long.MAX_VALUE};

        IndexEntryList<long[], String> results;
        for (int i = 0; i < 10000; i++) {
            results = phTree.getRange(start, end);
            assertEquals(nrEntries, results.size());
        }
    }

    @Test
    public void testRandomKNNBug() {
        phTree.create(2, 64);

        Random random = new Random(42);
        List<long[]> points = new ArrayList<>();
        for (int i = 0; i < 100; i++) {

            long[] key = randomKey(random);
            points.add(key);
            phTree.put(key, new BigInteger(64, random).toString());
        }
        int k = 3;
        long[] q = {-1170105035, 234785527};
        List<long[]> nearestNeighbors = MultidimUtil.knnToList(phTree.getNearestNeighbors(q, k));
        equalsList(MultidimUtil.nearestNeighboursBruteForce(q, k, points), nearestNeighbors);
    }

    @Test
    public void testRandomInsertAndKNN() {
        phTree.create(2, 64);

        Random random = new Random(42);
        List<long[]> points = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            long[] key = randomKey(random);
            points.add(key);
            phTree.put(key, new BigInteger(64, random).toString());
        }

        int k = 3;
        for (int i = 0; i < 100; i++) {
            long[] q = randomKey(random);
            System.out.println(Arrays.toString(q));
            List<long[]> nearestNeighbors = 
            		MultidimUtil.knnToList(phTree.getNearestNeighbors(q, k));
            equalsList(MultidimUtil.nearestNeighboursBruteForce(q, k, points), nearestNeighbors);
        }
    }

    private void equalsList(List<long[]> a, List<long[]> b) {
        assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            assertArrayEquals(a.get(i), b.get(i));
        }
    }

    private static long[] randomKey(Random random) {
        return new long[] { random.nextInt(), random.nextInt() };
    }

    @Test
    public void testStats() {
        phTree.create(2, 64);
        phTree.put(k(1, 1), "one");
        phTree.put(k(1, -1), "two");
        phTree.put(k(-1, 1), "three");
        phTree.put(k(-1, -1), "four");
        PhTreeHelper.Stats stats = phTree.getStats();
        System.out.println(stats);
    }

    @Test
    public void testStats_Ideal() {
        phTree.create(2, 64);
        phTree.put(k(1, 1), "one");
        phTree.put(k(1, -1), "two");
        phTree.put(k(-1, 1), "three");
        phTree.put(k(-1, -1), "four");
        PhTreeHelper.Stats stats = phTree.getStatsIdealNoNode();
        System.out.println(stats);
    }

    @Test
    public void testQuality() {
        phTree.create(2, 64);
        phTree.put(k(1, 1), "one");
        phTree.put(k(1, -1), "two");
        phTree.put(k(-1, 1), "three");
        phTree.put(k(-1, -1), "four");
        PhTreeQStats quality = phTree.getQuality();
        System.out.println(quality);
    }

    @Test
    public void testNodeCount() {
        phTree.create(2, 64);
        phTree.put(k(1, 1), "one");
        phTree.put(k(1, -1), "two");
        phTree.put(k(-1, 1), "three");
        phTree.put(k(-1, -1), "four");
        int nodeCount = phTree.getNodeCount();
        System.out.println(nodeCount);
    }

    @Test
    public void testToString() {
        phTree.create(2, 64);
        phTree.put(k(1, 1), "one");
        phTree.put(k(1, -1), "two");
        phTree.put(k(-1, 1), "three");
        phTree.put(k(-1, -1), "four");
        String toString = phTree.toStringPlain();
        System.out.println(toString);
    }

    @Test
    public void testSimple() throws Exception {
        phTree.create(2, 64);

        assertEquals(0, phTree.size());
        assertEquals(2, phTree.getDim());
        assertEquals(64, phTree.getDepth());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testKNNWithFilter() {
        phTree.create(2, 64);
        phTree.getNearestNeighbuor(1, null, null, null);
    }

    @Test
    public void testRangeList() {
        phTree.create(2, 64);
        IndexEntryList<long[], String> expected = setupTestTreeForRangeQueriesAndReturnExpectedResult();

        List<PhEntry<String>> pvEntries = phTree.queryAll(new long[]{9, 9}, new long[]{11, 11});
        final IndexEntryList<long[], String> results = new IndexEntryList<>();
        pvEntries.stream().forEach(e -> results.add(e.getKey(), e.getValue()));

        expected = MultidimUtil.sort(expected);
        IndexEntryList<long[], String> actual = MultidimUtil.sort(results);
        assertEqualsEntryLists(expected, actual);
    }

    @Test
    public void testRangeListFilterMapper() {
        phTree.create(2, 64);
        IndexEntryList<long[], String> expected = setupTestTreeForRangeQueriesAndReturnExpectedResult();
        long[] min = {9, 9};
        long[] max = {11, 11};
        List<PhEntry<String>> pvEntries = phTree.queryAll(min, max, 100, PhPredicate.ACCEPT_ALL, PhMapper.PVENTRY());
        final IndexEntryList<long[], String> results = new IndexEntryList<>();
        pvEntries.stream().forEach(e -> results.add(e.getKey(), e.getValue()));

        expected = MultidimUtil.sort(expected);
        IndexEntryList<long[], String> actual = MultidimUtil.sort(results);
        assertEqualsEntryLists(expected, actual);
    }

    @Test
    public void testRangeListFilterMapper_LongArray() {
        phTree.create(2, 64);

        IndexEntryList<long[], String> entries = setupTestTreeForRangeQueriesAndReturnExpectedResult();
        long[] min = {9, 9};
        long[] max = {11, 11};
        List<long[]> results = phTree.queryAll(min, max, 100, PhPredicate.ACCEPT_ALL, PhMapperK.LONG_ARRAY());

        entries = MultidimUtil.sort(entries);
        List<long[]> expected = new ArrayList<>();
        entries.stream().forEach( e -> expected.add(e.getKey()));

        results = MultidimUtil.sort(results);
        assertEquals(expected.size(), results.size());
        for (int i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i), results.get(i));
        }
    }

    @Test
    public void testRangeListFilterMapper_Value() {
        phTree.create(2, 64);

        IndexEntryList<long[], String> entries = setupTestTreeForRangeQueriesAndReturnExpectedResult();
        long[] min = {9, 9};
        long[] max = {11, 11};
        List<String> results = phTree.queryAll(min, max, 100, PhPredicate.ACCEPT_ALL, PhMapperV.VALUE());

        final HashSet<String> expected = new HashSet<>();
        entries.forEach(e -> expected.add(e.getValue()));
        final HashSet<String> resultSet = new HashSet<>(results);
        assertEquals(expected, resultSet);
    }


    @Test
    public void testUpdateKey() {
        phTree.create(2, 64);
        long[] key = {1, 2};
        long[] key2 = {-1, -2};
        String value = "Hello, world";
        phTree.put(key, value);
        assertEquals(value, phTree.get(key));
        assertNull(phTree.get(key2));
        phTree.update(key, key2);

        assertEquals(value, phTree.get(key2));
        assertNull(phTree.get(key));
    }

    private IndexEntryList<long[], String> setupTestTreeForRangeQueriesAndReturnExpectedResult() {
        phTree.put(new long[] {10, 10}, "foo");
        phTree.put(new long[] {11, 10}, "foo");
        phTree.put(new long[]{9, 10}, "foo");
        phTree.put(new long[]{10, 9}, "foo");
        phTree.put(new long[]{10, 11}, "foo");
        phTree.put(new long[]{10, 12}, "foo");
        phTree.put(new long[]{9, 7}, "foo");
        phTree.put(new long[]{10, 9}, "foo");
        phTree.put(new long[]{100000000, -1}, "foo");
        phTree.put(new long[]{-1, 100000000}, "foo");

        IndexEntryList<long[], String> expected = new IndexEntryList<>();
        expected.add(new IndexEntry<>(new long[] { 9L, 10L}, "foo"));
        expected.add(new IndexEntry<>(new long[] { 10L, 9L}, "foo"));
        expected.add(new IndexEntry<>(new long[] { 10L, 10L}, "foo"));
        expected.add(new IndexEntry<>(new long[] { 10L, 11L}, "foo"));
        expected.add(new IndexEntry<>(new long[] { 11L, 10L}, "foo"));
        return expected;
    }

    private <V> void assertEqualsEntryLists(IndexEntryList<long[], V> expected, IndexEntryList<long[], V> actual) {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            assertArrayEquals(expected.get(i).getKey(), actual.get(i).getKey());
            assertEquals(expected.get(i).getValue(), actual.get(i).getValue());
        }
    }

    private static long[] k(long... keys) {
        return keys;
    }
}
