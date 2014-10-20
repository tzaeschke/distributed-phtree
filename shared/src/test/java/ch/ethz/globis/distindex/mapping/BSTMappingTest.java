package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class BSTMappingTest {

    @Test
    public void testMapping() {
        KeyMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter());
        String[] hosts = { "one", "two", "three", "four", "five" };
        for (int i = 0; i < hosts.length; i++) {
            mapping.add(hosts[i]);
            assertEquals(Arrays.asList(hosts).subList(0, i + 1), mapping.getHostIds());
        }
    }
}
