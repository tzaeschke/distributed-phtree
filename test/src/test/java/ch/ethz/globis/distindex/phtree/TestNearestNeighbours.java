package ch.ethz.globis.distindex.phtree;

import ch.ethz.globis.distindex.BaseParameterizedTest;
import ch.ethz.globis.distindex.client.pht.DistributedPHTreeProxy;
import ch.ethz.globis.distindex.client.pht.DistributedPhTreeV;
import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.pht.*;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

public class TestNearestNeighbours extends BaseParameterizedTest {

    private static final long EPSILON = 100;

    private PhDistance metric = new PhDistanceL();

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { {16} });
    }

    private PHFactory factory;

    public TestNearestNeighbours(int nrServers) throws IOException {
        super(nrServers);
        factory = new PHFactory(HOST, ZK_PORT);
    }

    /**
     * Configuration presented below:
     *
     +----+----+----+----+
     |    |    |    |    |
     |    |    |    |    |
     +--------------B----+
     |    |    |    |    |
     |    |    |    |    |
     +---------Q----AC---+
     |    |    |    |    |
     |    |    |    |    |
     +-------------------+
     |    |    |    |    |
     |    |    |    |    |
     +----+----+----+----+
     *  The space is split evenly between 16 hosts and the points contained are A, B and C. Q is the query point
     *  for kNN but it is not stored.
     *
     *  dist(Q, A) = side
     *  dist(Q, B) = side * sqrt(2)
     *  dist(Q, C) = side + 1
     *
     *  The two nearest neighbours of Q are A and C.
     */
    @Test
    public void testFind16Hosts_Across1() {
        DistributedPHTreeProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = Long.MAX_VALUE * 1/2;
        long[] A = {side, 0};
        long[] B = {side, side};
        long[] C = {side + 1, 0};
        long[] Q = {0, 0};
        assertTrue("Configuration not set up properly. C should be closer to Q than B.", metric.dist(Q, B) > metric.dist(Q, C));

        tree.insert(A);
        tree.insert(B);
        tree.insert(C);

        List<long[]> result = tree.nearestNeighbour(2, Q);
        assertEquals(2, result.size());
        checkContains(result, C);
        checkContains(result, A);
    }

    /**
     * Configuration presented below:
     *
     +----+----+----+----+
     |    |    |    |    |
     |    |    |    |    |
     +--------------B----+
     |    |    |    |    |
     |    |    |    |    |
     +----C----Q----A----+
     |    |    |    |    |
     |    |    |    |    |
     +-------------------+
     |    |    |    |    |
     |    |    |    |    |
     +----+----+----+----+
     *  The space is split evenly between 16 hosts and the points contained are A, B and C. Q is the query point
     *  for kNN but it is not stored.
     *
     *  dist(Q, A) = side
     *  dist(Q, B) = side * sqrt(2)
     *  dist(Q, C) = side + 1
     *
     *  The two nearest neighbours of Q are A and C.
     */
    @Test
    public void testFind16Hosts_Across2() {
        DistributedPHTreeProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = Long.MAX_VALUE * 1/2;
        long[] A = {side, 0};
        long[] B = {side, side};
        long[] C = {-side - 1, 0};
        long[] Q = {0, 0};
        assertTrue("Configuration not set up properly. C should be closer to Q than B.", metric.dist(Q, B) > metric.dist(Q, C));
        tree.insert(A);
        tree.insert(B);
        tree.insert(C);

        List<long[]> result = tree.nearestNeighbour(2, Q);
        assertEquals("Invalid result size", 2, result.size());
        checkContains(result, C);
        checkContains(result, A);
    }

    /**
     * Configuration presented below:
     *
     +----+----+----+----+
     |    |    |    |    |
     |    |    |    |    |
     +--------------B----+
     |    |    |    |    |
     |    |    |    |    |
     +---------Q----A----+
     |    |    |    |    |
     |    |    |    |    |
     +---------C---------+
     |    |    |    |    |
     |    |    |    |    |
     +----+----+----+----+
     *  The space is split evenly between 16 hosts and the points contained are A, B and C. Q is the query point
     *  for kNN but it is not stored.
     *
     *  dist(Q, A) = side
     *  dist(Q, B) = side * sqrt(2)
     *  dist(Q, C) = side + 1
     *
     *  The two nearest neighbours of Q are A and C.
     */
    @Test
    public void testFind16Hosts_Across3() {
        DistributedPHTreeProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = Long.MAX_VALUE * 1/2;
        long[] A = {side, 0};
        long[] B = {side, side};
        long[] C = {0, -side-1};
        long[] Q = {0, 0};
        assertTrue("Configuration not set up properly. C should be closer to Q than B.", metric.dist(Q, B) > metric.dist(Q, C));
        tree.insert(A);
        tree.insert(B);
        tree.insert(C);

        List<long[]> result = tree.nearestNeighbour(2, Q);
        assertEquals(2, result.size());
        checkContains(result, C);
        checkContains(result, A);
    }

    /**
     * Configuration presented below:
     *
     +----+----+----+----+
     |    |    |    |    |
     |    |    |    |    |
     +---------C----B----+
     |    |    |    |    |
     |    |    |    |    |
     +---------Q----A----+
     |    |    |    |    |
     |    |    |    |    |
     +-------------------+
     |    |    |    |    |
     |    |    |    |    |
     +----+----+----+----+
     *  The space is split evenly between 16 hosts and the points contained are A, B and C. Q is the query point
     *  for kNN but it is not stored.
     *
     *  dist(Q, A) = side
     *  dist(Q, B) = side * sqrt(2)
     *  dist(Q, C) = side
     *
     *  The two nearest neighbours of Q are A and C.
     */
    @Test
    public void testFind16Hosts_Across4() {
        DistributedPHTreeProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = Long.MAX_VALUE * 1/2;
        long[] A = {side, 0};
        long[] B = {side, side};
        long[] C = {0, side + 1};
        long[] Q = {0, 0};
        assertTrue("Configuration not set up properly. C should be closer to Q than B.", metric.dist(Q, B) > metric.dist(Q, C));
        tree.insert(A);
        tree.insert(B);
        tree.insert(C);

        List<long[]> result = tree.nearestNeighbour(2, Q);
        assertEquals(2, result.size());
        checkContains(result, C);
        checkContains(result, A);
    }

    /**
     * Configuration presented below:
     *
     +----+----+----+----+
     |    |    |    |    |
     |    |    |    |    |
     +--------------B----+
     |    |    |    |    |
     |    |    |    |    |
     +---------Q----A----+
     |    |    |    |    |
     |    |    |    |    |
     +-------------------+
     |    |    C    |    |
     |    |    |    |    |
     +----+----+----+----+
     *  The space is split evenly between 16 hosts and the points contained are A, B and C. Q is the query point
     *  for kNN but it is not stored.
     *
     *  dist(Q, A) = side
     *  dist(Q, B) = side * sqrt(2)
     *  dist(Q, C) = side * sqrt(2) - EPSILON
     *
     *  The two nearest neighbours of Q are A and C.
     */
    @Test
    public void testFind16Hosts_Across5() {
        DistributedPHTreeProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = Long.MAX_VALUE * 1/2;
        long[] A = {side, 0};
        long[] B = {side, side};
        long radius = (long) (side * Math.sqrt(2));
        long[] C = {0, - radius + EPSILON};
        long[] Q = {0, 0};
        assertTrue("Configuration not set up properly. C should be closer to Q than B.", metric.dist(Q, B) > metric.dist(Q, C));
        tree.insert(A);
        tree.insert(B);
        tree.insert(C);

        List<long[]> result = tree.nearestNeighbour(2, Q);
        assertEquals(2, result.size());
        checkContains(result, C);
        checkContains(result, A);
    }

    /**
     * Configuration presented below:
     *
     +----+----+----+----+
     |    |    |    |    |
     |    |    |    |    |
     +--------------B----+
     |    |    |    |    |
     |    |    |    |    |
     +---------Q----AC---+
     |    |    |    |    |
     |    |    |    |    |
     +-------------------+
     |    |    |    |    |
     |    |    |    |    |
     +----+----+----+----+
     *  The space is split evenly between 16 hosts and the points contained are A, B and C. Q is the query point
     *  for kNN but it is not stored.
     *
     *  dist(Q, A) = side
     *  dist(Q, B) = side * sqrt(2)
     *  dist(Q, C) = side * sqrt(2) - EPSILON
     *
     *  The two nearest neighbours of Q are A and C.
     */
    @Test
    public void testFind16Hosts_Across6() {
        DistributedPHTreeProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = Long.MAX_VALUE * 1/2;
        long[] A = {side, 0};
        long[] B = {side, side};
        long radius = (long) (side * Math.sqrt(2));
        long[] C = {radius - EPSILON, 0};
        long[] Q = {0, 0};
        assertTrue("Configuration not set up properly. C should be closer to Q than B.", metric.dist(Q, B) > metric.dist(Q, C));
        tree.insert(A);
        tree.insert(B);
        tree.insert(C);

        List<long[]> result = tree.nearestNeighbour(2, Q);
        assertEquals(2, result.size());
        checkContains(result, C);
        checkContains(result, A);
    }

    private double[] sinCostForAngle(int angleInDegrees) {
        double angleInRadians = Math.toRadians(angleInDegrees);
        return new double[] { Math.sin(angleInRadians), Math.cos(angleInRadians)};
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
        fail("Point not found in results: " + Arrays.toString(v));
    }

    private String resultsToString(List<long[]> results) {
        String out = "[";
        for (long[] point : results) {
            out += Arrays.toString(point) + " ";
        }
        out += "]";
        return out;
    }

    private String resultsToStringDouble(List<double[]> results) {
        String out = "[";
        for (double[] point : results) {
            out += arrayToString(point) + " ";
        }
        out += "]";
        return out;
    }

    private String arrayToString(double[] array) {
        int length = array.length;
        String out = "[";
        for (int i = 0; i < length; i++) {
            out += String.format("%.3f", array[i]);
            if (i != length - 1) {
                out += ", ";
            }
        }
        out += "]";
        return out;
    }
}
