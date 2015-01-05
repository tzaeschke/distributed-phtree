package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.mapping.bst.BST;
import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static ch.ethz.globis.distindex.mapping.util.TestOperationsUtil.assertEqualsListVararg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test any operations that are
 */
@Ignore
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
        mapping.add(hostId2);

        mapping.split(hostId1, hostId3, sizeMoved);
        sizeHostId1 -= sizeMoved;
        sizeHostId3 = sizeMoved;
        BST internal = mapping.getBst();
        assertEquals(sizeHostId1, internal.findFirstByContent(hostId1).getSize());
        assertEquals(sizeHostId2, internal.findFirstByContent(hostId2).getSize());
        assertEquals(sizeHostId3, internal.findFirstByContent(hostId3).getSize());
    }

    @Test
    public void testGetDepth() {
        BSTMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter());
        int depth = 10;
        int size = (int) Math.pow(2, depth);
        String[] hosts = new String[size];
        for (int i = 0; i < size; i++) {
            hosts[i] = String.valueOf(i);
            mapping.add(hosts[i]);
        }
        for (String hostId : hosts) {
            assertEquals(depth + 1, mapping.getDepth(hostId));
        }
    }

    @Test
    public void testSetSize() {
        int size = 16;
        BSTMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter());
        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = String.valueOf(i);
            mapping.add(keys[i]);
            //mapping.setSize(keys[i], i + 1);
        }

        for (int i = 0; i < size; i++) {
            int sz = mapping.getBst().findFirstByContent(keys[i]).getSize();
            assertEquals(i + 1, sz);
        }
    }

    @Test
    public void testSplittingCandidates() {
        int nrHosts = 200;
        BSTMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter());
        String hostId;
        for (int i = 0; i < nrHosts; i++) {
            hostId = toHostId(i);
            mapping.add(hostId);
        }

        String candidate;
        for (int i = nrHosts - 1; i >= 0; i--) {
            hostId = toHostId(i);
            //mapping.setSize(hostId, 2 * nrHosts - i);
            candidate = mapping.getHostForSplitting(toHostId(i + 1));
            assertEquals(hostId, candidate);
        }
    }

    @Test
    public void testSplittingCandidates_NotCurrentHostId() {
        BSTMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter());
        String currentHostId = "0";
        mapping.add(currentHostId);
        String candidate = mapping.getHostForSplitting(currentHostId);
        assertNull(candidate);
    }


    @Test
    public void testMapping() {
        String[] hosts = { "one", "two", "three", "four", "five" };
        KeyMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter(), hosts);
        assertEquals(Arrays.asList(hosts).subList(0, hosts.length), mapping.get());
    }

    @Test
    public void testTreeCreation() {
        int size = 16;
        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = String.valueOf(i);
        }
        BSTMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter(), keys);
        assertEqualsListVararg(mapping.get(), keys);
    }

    @Test
    public void testRangeQuery() {
        String[] hosts = { "one", "two", "three", "four" };
        BSTMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter(), hosts);
        Map<String, String> directMapping = mapping.asMap();
        long[] start = {1, 1};
        long[] end = {2, 2};
        List<String> hostIds = mapping.get(start, end);
        assertEqualsListVararg(hostIds, directMapping.get("00"));

        start = new long[] {-1, -1};
        end = new long[] {-5, -5};
        hostIds = mapping.get(start, end);
        assertEqualsListVararg(hostIds, directMapping.get("11"));

        start = new long[] {-1, 1};
        end = new long[] {-5, 5};
        hostIds = mapping.get(start, end);
        assertEqualsListVararg(hostIds, directMapping.get("10"));

        start = new long[] {1, -1};
        end = new long[] {5, -5};
        hostIds = mapping.get(start, end);
        assertEqualsListVararg(hostIds, directMapping.get("01"));

        start = new long[] {-1, -1};
        end = new long[] {5, 5};
        hostIds = mapping.get(start, end);
        assertEqualsListVararg(hostIds, directMapping.get("00"), directMapping.get("01"),
                directMapping.get("10"), directMapping.get("11"));
    }

    private String toHostId(int i) {
        return String.valueOf(i);
    }
}
