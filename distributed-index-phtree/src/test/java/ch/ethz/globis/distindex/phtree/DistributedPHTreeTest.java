package ch.ethz.globis.distindex.phtree;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DistributedPHTreeTest {

    @Test
    public void testPutAndGet() {
        String host = "localhost";
        int port = 2000;
        DistributedPHTree<String> phTree = new DistributedPHTree<>(host, port);

        long[] key = new long[] {1, 2};
        String value = "hello";

        phTree.put(key, value);

        String retrieved = phTree.get(key);
        assertEquals("Wrong value retrieval", value, retrieved);
    }

}