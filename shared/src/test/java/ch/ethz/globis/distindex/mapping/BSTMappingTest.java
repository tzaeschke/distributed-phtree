package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class BSTMappingTest {

    @Test
    public void testMapping() {
        KeyMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter(), new String[]{});
        String[] hosts = { "one", "two", "three", "four", "five" };
        for (int i = 0; i < hosts.length; i++) {
            mapping.add(hosts[i]);
            assertEquals(Arrays.asList(hosts).subList(0, i + 1), mapping.getHostIds());
        }
    }

    @Test
    public void testTreeCreation() {
        int size = 16;
        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = String.valueOf(i);
        }
        KeyMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter(), keys);
        System.out.println(mapping.getHosts());
    }
}
