package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.test.BaseParameterizedTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DistPhTreeSmallerSize extends BaseParameterizedTest {

    private PHTreeIndexProxy<Integer> phTree;

    public DistPhTreeSmallerSize(int nrServers) throws IOException {
        super(nrServers, true);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{4}});
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
    public void testInsert_8bits() {
        long[][] keys = {
                {0b00100000, 0b10110101},//   v=null
                {0b00100001, 0b10110100},//   v=null
        };
        phTree.create(2, 8);

        phTree.put(keys[0], 0);
        assertNotNull(phTree.get(keys[0]));
        assertEquals(0, (int) phTree.get(keys[0]));

        phTree.put(keys[1], 1);

        assertNotNull(phTree.get(keys[0]));
        assertEquals(0, (int) phTree.get(keys[0]));
        assertNotNull(phTree.get(keys[1]));
        assertEquals(1, (int) phTree.get(keys[1]));
    }

    @Test
    public void testInsert_32bits() throws InterruptedException {
        phTree.create(2, 32);
        long[][] keys = {
                {Integer.MAX_VALUE, Integer.MAX_VALUE},
                {Integer.MIN_VALUE, Integer.MAX_VALUE},
                {Integer.MAX_VALUE, Integer.MIN_VALUE},
                {Integer.MIN_VALUE, Integer.MIN_VALUE}
        };
        for (long[] key : keys) {
            System.out.println("Searching for " + Arrays.toString(key));
            phTree.put(key, 0);
            assertTrue(phTree.contains(key));
        }
    }
}