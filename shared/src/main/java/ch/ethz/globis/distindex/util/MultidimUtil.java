package ch.ethz.globis.distindex.util;

import ch.ethz.globis.pht.PhTree;

import java.math.BigDecimal;
import java.util.*;

public class MultidimUtil {

    public static List<long[]> nearestNeighboursBruteForce(final long[] q, int k, List<long[]> points) {
        Collections.sort(points, new Comparator<long[]>() {
            @Override
            public int compare(long[] a, long[] b) {
                return distance(a, q).compareTo(distance(b, q));
            }
        });
        return points.subList(0, k);
    }

    private static BigDecimal distance(final long[] a, final long[] b) {
        BigDecimal dist = new BigDecimal(0);
        for (int i = 0; i < a.length; i++) {
            BigDecimal d = new BigDecimal(a[i]).subtract(new BigDecimal(b[i]));
            dist = dist.add(d.pow(2));
        }
        return dist;
    }

    public static List<long[]> nearestNeighbours(long[] q, int k, List<long[]> points) {
        if (points.size() == 0) {
            return new ArrayList<>();
        }
        PhTree tree = createTree(points);
        return tree.nearestNeighbour(k, q);
    }

    public static List<long[]> sort(List<long[]> points) {
        if (points.size() == 0) {
            return new ArrayList<>();
        }

        PhTree tree = createTree(points);

        List<long[]> output = new ArrayList<>();
        Iterator<long[]> it = tree.queryExtent();
        while (it.hasNext()) {
            output.add(it.next());
        }
        return output;
    }

    private static PhTree createTree(List<long[]> points) {
        int dim = points.get(0).length;

        PhTree tree = PhTree.create(dim, 64);
        for (long[] point : points) {
            tree.insert(point);
        }
        return tree;
    }

    public static long computeDistance(long[] a, long[] b) {
        long dist = 0;
        for (int i = 0; i < a.length; i++) {
            dist += (a[i] - b[i]) * (a[i] - b[i]);
        }
        return (long) Math.sqrt(dist) + 1;
    }

    public static long[] transpose(long[] a, long offset) {
        long[] result = Arrays.copyOf(a, a.length);
        for (int i = 0; i < a.length; i++) {
            result[i] += offset;
        }
        return result;
    }

    public static long[] transpose(long[] a, double offset) {
        long[] result = Arrays.copyOf(a, a.length);
        for (int i = 0; i < a.length; i++) {
            result[i] += offset;
        }
        return result;
    }
}
