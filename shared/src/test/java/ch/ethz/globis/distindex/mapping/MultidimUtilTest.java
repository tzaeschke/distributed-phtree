package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.util.MultidimUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MultidimUtilTest {

    @Test
    public void testNearestNeighboursComparison() {
        List<long[]> points = new ArrayList<long[]>() {{
            add(new long[]{1, 1});
            add(new long[]{2, 2});
            add(new long[]{3, 3});
        }};
        long[] q = {0, 0};
        List<long[]> result = MultidimUtil.nearestNeighbours(q, 1, points);
        List<long[]> resultB = MultidimUtil.nearestNeighboursBruteForce(q, 1, points);

        equalsList(result, resultB);
        equalsList(result, points.subList(0, 1));

        result = MultidimUtil.nearestNeighbours(q, 2, points);
        resultB = MultidimUtil.nearestNeighboursBruteForce(q, 2, points);

        equalsList(result, resultB);
        equalsList(result, points.subList(0, 2));
    }

    @Test
    public void testRandomInsertAndKNN() {
        Random random = new Random();
        List<long[]> points = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            long[] key = { random.nextLong(), random.nextLong() };
            points.add(key);
        }

        int k = 10;
        for (int i = 0; i < 100; i++) {
            long[] q = {random.nextLong(), random.nextLong() };
            List<long[]> nearestNeighbors = MultidimUtil.nearestNeighbours(q, k, points);
            equalsList(MultidimUtil.nearestNeighboursBruteForce(q, k, points), nearestNeighbors);
        }
    }

    @Test
    public void testNextLong() {
        long[] key = {0, 0};
        int size = Long.SIZE;
        assertArrayEquals(new long[]{0, 1}, MultidimUtil.next(key, size));

        key = new long[] {0, 1};
        assertArrayEquals(new long[]{1, 0}, MultidimUtil.next(key, size));

        key = new long[] {1, 1};
        assertArrayEquals(new long[]{0, 2}, MultidimUtil.next(key, size));

        key = new long[] {3, 3};
        size = 2;
        assertArrayEquals(new long[]{0, 0}, MultidimUtil.next(key, size));

        key = new long[] {Long.MAX_VALUE, Long.MAX_VALUE};
        size = Long.SIZE;
        assertArrayEquals(new long[]{0,  -1 * (Long.MAX_VALUE) - 1}, MultidimUtil.next(key, size));
    }

    @Test
    public void testPreviousLong() {
        long[] key = {0, 1};
        int size = Long.SIZE;
        assertArrayEquals(new long[] {0, 0}, MultidimUtil.previous(key, size));

        key = new long[] {1, 0};
        assertArrayEquals(new long[] {0, 1}, MultidimUtil.previous(key, size));

        key = new long[] {0, 2};
        assertArrayEquals(new long[] {1, 1}, MultidimUtil.previous(key, size));

        key = new long[] {0, 0};
        size = 2;
        assertArrayEquals(new long[] {3, 3}, MultidimUtil.previous(key, size));

        key = new long[] {0,  -1 * (Long.MAX_VALUE) - 1};
        size = Long.SIZE;
        assertArrayEquals(new long[]{Long.MAX_VALUE, Long.MAX_VALUE}, MultidimUtil.previous(key, size));
    }

    private void equalsList(List<long[]> a, List<long[]> b) {
        assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            assertArrayEquals(a.get(i), b.get(i));
        }
    }
}

