package ch.ethz.globis.distindex.phtree;

import ch.ethz.globis.distindex.BaseParameterizedTest;
import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.pht.Bits;
import ch.ethz.globis.pht.PhTree;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestNearestNeighbours extends BaseParameterizedTest {

    private PHFactory factory;

    public TestNearestNeighbours(int nrServers) throws IOException {
        super(nrServers);
        factory = new PHFactory(HOST, ZK_PORT);
    }
    
    @Test
    public void testDirectHit() {
        PhTree idx = factory.createPHTreeSet(2, 8);
        idx.insert(new long[]{2,2});
        idx.insert(new long[]{1,1});
        idx.insert(new long[]{1,3});
        idx.insert(new long[]{3,1});

        List<long[]> result = idx.nearestNeighbour(0, 3, 3);
        assertTrue(result.isEmpty());

        result = idx.nearestNeighbour(1, 2, 2);
        assertEquals(1, result.size());
        check(8, result.get(0), 2, 2);

        result = idx.nearestNeighbour(1, 1, 1);
        assertEquals(1, result.size());
        check(8, result.get(0), 1, 1);

        result = idx.nearestNeighbour(1, 1, 3);
        assertEquals(1, result.size());
        check(8, result.get(0), 1, 3);

        result = idx.nearestNeighbour(1, 3, 1);
        assertEquals(1, result.size());
        check(8, result.get(0), 3, 1);
    }

    @Test
    public void testNeighbour1of4() {
        PhTree idx = factory.createPHTreeSet(2, 8);
        idx.insert(new long[]{2,2});
        idx.insert(new long[]{1,1});
        idx.insert(new long[]{1,3});
        idx.insert(new long[]{3,1});

        List<long[]> result = idx.nearestNeighbour(1, 3, 3);
        check(8, result.get(0), 2, 2);
        assertEquals(1, result.size());
    }

    @Test
    public void testNeighbour1of5DirectHit() {
        PhTree idx = factory.createPHTreeSet(2, 8);
        idx.insert(new long[]{3,3});
        idx.insert(new long[]{2,2});
        idx.insert(new long[]{1,1});
        idx.insert(new long[]{1,3});
        idx.insert(new long[]{3,1});

        List<long[]> result = idx.nearestNeighbour(1, 3, 3);
        check(8, result.get(0), 3, 3);
        assertEquals(1, result.size());
    }

    @Test
    public void testNeighbour4_5of4() {
        PhTree idx = factory.createPHTreeSet(2, 8);
        idx.insert(new long[]{3,3});
        idx.insert(new long[]{2,2});
        idx.insert(new long[]{4,4});
        idx.insert(new long[]{2,4});
        idx.insert(new long[]{4,2});

        List<long[]> result = idx.nearestNeighbour(4, 3, 3);

        checkContains(result, 3, 3);
        checkContains(result, 4, 4);
        checkContains(result, 4, 2);
        checkContains(result, 2, 2);
        checkContains(result, 2, 4);

        assertEquals(5, result.size());
    }

    private void check(int DEPTH, long[] t, long ... ints) {
        for (int i = 0; i < ints.length; i++) {
            assertEquals("i=" + i + " | " + Bits.toBinary(ints, DEPTH) + " / " +
                    Bits.toBinary(t, DEPTH), ints[i], t[i]);
        }
    }

    private void checkContains(List<long[]> l, long ... v) {
        for (long[] vl: l) {
            if (Arrays.equals(vl, v)) {
                return;
            }
        }
        fail("Not found: " + Arrays.toString(v));
    }
}
