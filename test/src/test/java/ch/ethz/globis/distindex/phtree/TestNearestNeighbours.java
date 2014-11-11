package ch.ethz.globis.distindex.phtree;

import ch.ethz.globis.distindex.BaseParameterizedTest;
import ch.ethz.globis.distindex.client.pht.DistributedPHTreeProxy;
import ch.ethz.globis.distindex.client.pht.DistributedPhTreeV;
import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.pht.*;
import ch.ethz.globis.pht.v3.PhTree3;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestNearestNeighbours extends BaseParameterizedTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { {16} });
    }

    private PHFactory factory;

    public TestNearestNeighbours(int nrServers) throws IOException {
        super(nrServers);
        factory = new PHFactory(HOST, ZK_PORT);
    }

    @Test
    public void testLargeCircle() {
        DistributedPHTreeProxy<Object> proxy = factory.createProxy(2, 64);
        PhTree tree = new PhTreeVProxy(new DistributedPhTreeV<>(proxy));
        KeyMapping<long[]> mapping = proxy.getMapping();
        System.out.println(mapping.getHosts());

        long side = Long.MAX_VALUE * 1/8;
        long radius = (long) (side * Math.sqrt(2));
        tree.insert(side, side);
        tree.insert(0, radius);
        tree.insert(radius, 0);
        tree.insert(-radius, 0);

        List<long[]> result = tree.nearestNeighbour(5, 0, 0);
        System.out.println(resultsToString(result));
    }

    @Test
    public void testUnitCircle() {
        PhTreeV<Object> idx = factory.createPHTreeMap(2, 64);
        PhTreeVD<Object> tree = new PhTreeVD<>(idx);
        tree.put(new double[]{0, 0}, null);
        tree.put(sinCostForAngle(30), null);
        tree.put(sinCostForAngle(60), null);
        tree.put(sinCostForAngle(90), null);
        tree.put(sinCostForAngle(120), null);
        tree.put(sinCostForAngle(150), null);
        tree.put(sinCostForAngle(180), null);
        tree.put(sinCostForAngle(210), null);
        tree.put(sinCostForAngle(240), null);
        tree.put(sinCostForAngle(270), null);
        tree.put(sinCostForAngle(300), null);
        tree.put(sinCostForAngle(330), null);
        List<double[]> result = tree.nearestNeighbour(3, 0, 0);
        System.out.println(resultsToStringDouble(result));
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
        fail("Not found: " + Arrays.toString(v));
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
