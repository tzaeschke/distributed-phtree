package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BSTMappingTest {

    @Test
    public void testMapping() {
//        KeyMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter(), new String[]{});
//        String[] hosts = { "one", "two", "three", "four", "five" };
//        for (int i = 0; i < hosts.length; i++) {
//            mapping.add(hosts[i]);
//            assertEquals(Arrays.asList(hosts).subList(0, i + 1), mapping.getHostIds());
//        }
    }

    @Test
    public void testTreeCreation() {
        int size = 16;
        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = String.valueOf(i);
        }
        KeyMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter(), keys);
        System.out.println(mapping.asMap());
    }

    @Test
    public void testTreeCreationIterative() {
        int size = 4;
        KeyMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter());
        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = String.valueOf(i);
            mapping.add(keys[i]);
        }
        System.out.println(mapping.asMap());
        for (int i = 0; i < size; i++) {
            mapping.remove(keys[i]);
            System.out.println(mapping.asMap());
        }
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

    private void assertEqualsListVararg(List<String> current, String... expected) {
        List<String> expectedList = Arrays.asList(expected);
        assertEquals(expectedList, current);
    }
}
