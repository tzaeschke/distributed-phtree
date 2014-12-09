package ch.ethz.globis.distindex.phtree;

import ch.ethz.globis.distindex.client.pht.DistributedPhTreeV;
import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.middleware.PhTreeRequestHandler;
import ch.ethz.globis.distindex.middleware.api.Middleware;
import ch.ethz.globis.distindex.middleware.net.IndexMiddleware;
import ch.ethz.globis.distindex.middleware.PhTreeIndexMiddlewareFactory;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.test.BaseParameterizedTest;
import ch.ethz.globis.distindex.test.SimpleClusterService;
import ch.ethz.globis.distindex.test.SimplePhFactory;
import ch.ethz.globis.pht.*;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class TestNearestNeighbours extends BaseParameterizedTest {

    private static final int BIT_WIDTH = 16;
    private static final long SQUARE_SIDE = Short.MAX_VALUE / 2;
    private static final long EPSILON = 100;

    private PhDistance metric = new PhDistanceL();
    private static ClusterService<long[]> clusterService = new SimpleClusterService(BIT_WIDTH);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { {16} });
    }

    private PHFactory factory;

    public TestNearestNeighbours(int nrServers) throws IOException {
        super(nrServers);
        factory = new SimplePhFactory(clusterService);
    }

    @Override
    protected Middleware createMiddleware(int i, String host, int port, String zkHost, int zkPort) {
        return PhTreeIndexMiddlewareFactory.newPhTree(host, port, clusterService);
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
        PHTreeIndexProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = SQUARE_SIDE;
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
        PHTreeIndexProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = SQUARE_SIDE;
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
        PHTreeIndexProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = SQUARE_SIDE;
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
        PHTreeIndexProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = SQUARE_SIDE;
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
        PHTreeIndexProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = SQUARE_SIDE;
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
        PHTreeIndexProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = SQUARE_SIDE;
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

    /**
     * Configuration presented below:
     *
     +----+----+----+----+
     |    |    |    |    |
     |    |    |    |    |
     +--------------B----+
     |    |    |    |    |
     |    |    |    |    |
     +---C-----Q----A----+
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
    public void testFind16Hosts_Across7() {
        PHTreeIndexProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = SQUARE_SIDE;
        long[] A = {side, 0};
        long[] B = {side, side};
        long radius = (long) (side * Math.sqrt(2));
        long[] C = {- radius + EPSILON, 0};
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
     |    |    C    |    |
     +--------------B----+
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
     *  dist(Q, C) = side * sqrt(2) - EPSILON
     *
     *  The two nearest neighbours of Q are A and C.
     */
    @Test
    public void testFind16Hosts_Across8() {
        PHTreeIndexProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = SQUARE_SIDE;
        long[] A = {side, 0};
        long[] B = {side, side};
        long radius = (long) (side * Math.sqrt(2));
        long[] C = {0, radius - EPSILON};
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

    @Test
    public void testFind16Hosts_NotEnough() {
        PHTreeIndexProxy<Object> proxy = factory.createProxy(2, 16);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));

        long side = SQUARE_SIDE;
        long[] A = {side * 2, 0};
        long[] B = {0, side * 2};

        long[] Q = {0, 0};

        tree.insert(A);
        tree.insert(B);

        List<long[]> result = tree.nearestNeighbour(2, Q);
        assertEquals(2, result.size());
        checkContains(result, B);
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
