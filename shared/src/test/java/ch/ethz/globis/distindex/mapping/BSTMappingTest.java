package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.mapping.bst.BST;
import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import ch.ethz.globis.distindex.mapping.bst.MultidimMapping;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static ch.ethz.globis.distindex.mapping.util.TestOperationsUtil.assertEqualsListVararg;
import static org.junit.Assert.assertEquals;

/**
 * Test any operations that are
 */
public class BSTMappingTest {

    @Test
    public void testSplit() {
        BSTMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter());
        String hostId1 = "1";
        String hostId2 = "2";
        String hostId3 = "3";
        int sizeHostId1 = 10;
        int sizeHostId2 = 2;
        int sizeHostId3;
        int sizeMoved = 5;

        mapping.add(hostId1);
        mapping.setSize(hostId1, sizeHostId1);
        mapping.add(hostId2);
        mapping.setSize(hostId2, sizeHostId2);

        mapping.split(hostId1, hostId3, sizeMoved);
        sizeHostId1 -= sizeMoved;
        sizeHostId3 = sizeMoved;
        BST<long[]> internal = mapping.getBst();
        assertEquals(sizeHostId1, internal.findByContent(hostId1).getSize());
        assertEquals(sizeHostId2, internal.findByContent(hostId2).getSize());
        assertEquals(sizeHostId3, internal.findByContent(hostId3).getSize());
    }

    @Test
    public void testSetSize() {
        int size = 16;
        BSTMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter());
        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = String.valueOf(i);
            mapping.add(keys[i]);
            mapping.setSize(keys[i], i + 1);
        }

        for (int i = 0; i < size; i++) {
            int sz = mapping.getBst().findByContent(keys[i]).getSize();
            assertEquals(i + 1, sz);
        }

    }

    @Test
    public void testMapping() {
        String[] hosts = { "one", "two", "three", "four", "five" };
        KeyMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter(), hosts);
        assertEquals(Arrays.asList(hosts).subList(0, hosts.length), mapping.getHostIds());
    }

    @Test
    public void testTreeCreation() {
        int size = 16;
        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = String.valueOf(i);
        }
        BSTMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter(), keys);
        assertEqualsListVararg(mapping.getHostIds(), keys);
    }

    @Test
    public void testRangeQuery() {
        String[] hosts = { "one", "two", "three", "four" };
        KeyMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter(), hosts);
        Map<String, String> directMapping = mapping.asMap();
        long[] start = {1, 1};
        long[] end = {2, 2};
        List<String> hostIds = mapping.getHostIds(start, end);
        assertEqualsListVararg(hostIds, directMapping.get("00"));

        start = new long[] {-1, -1};
        end = new long[] {-5, -5};
        hostIds = mapping.getHostIds(start, end);
        assertEqualsListVararg(hostIds, directMapping.get("11"));

        start = new long[] {-1, 1};
        end = new long[] {-5, 5};
        hostIds = mapping.getHostIds(start, end);
        assertEqualsListVararg(hostIds, directMapping.get("10"));

        start = new long[] {1, -1};
        end = new long[] {5, -5};
        hostIds = mapping.getHostIds(start, end);
        assertEqualsListVararg(hostIds, directMapping.get("01"));

        start = new long[] {-1, -1};
        end = new long[] {5, 5};
        hostIds = mapping.getHostIds(start, end);
        assertEqualsListVararg(hostIds, directMapping.get("00"), directMapping.get("01"),
                directMapping.get("10"), directMapping.get("11"));
    }
}
