package ch.ethz.globis.distindex.mapping;

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
