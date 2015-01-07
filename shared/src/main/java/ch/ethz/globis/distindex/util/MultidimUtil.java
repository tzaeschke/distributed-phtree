package ch.ethz.globis.distindex.util;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.mapping.zorder.ZAddress;
import ch.ethz.globis.distindex.mapping.zorder.ZOrderService;
import ch.ethz.globis.pht.PVEntry;
import ch.ethz.globis.pht.PVIterator;
import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.PhTreeV;
import ch.ethz.globis.pht.v4.PhTree4;

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

    public static <V> IndexEntryList<long[], V> sort(IndexEntryList<long[], V> entries) {
        if (entries.size() == 0) {
            return new IndexEntryList<>();
        }
        PhTreeV<V> tree = createTree(entries);
        IndexEntryList<long[], V> output = new IndexEntryList<>();
        PVIterator<V> it = tree.queryExtent();
        while (it.hasNext()) {
            PVEntry<V> entry = it.nextEntry();
            output.add(entry.getKey(), entry.getValue());
        }
        return output;
    }

    private static <V> PhTreeV<V> createTree(IndexEntryList<long[], V> entries) {
        int dim = entries.get(0).getKey().length;
        PhTreeV<V> tree = new PhTree4<>(dim, Long.SIZE);
        for (IndexEntry<long[], V> entry : entries) {
            tree.put(entry.getKey(), entry.getValue());
        }
        return tree;
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

    public static long[] next(long[] key, int depth) {
        long[] nextKey = Arrays.copyOf(key, key.length);
        for (int bitPos = 0; bitPos < depth; bitPos++) {
            for (int dimPos = key.length - 1; dimPos >= 0; dimPos--) {
                if ((nextKey[dimPos] & (1L << bitPos)) != 0) {
                    //bit was 1, set to 0 and continue
                    nextKey[dimPos] = nextKey[dimPos] & ~(1L << bitPos);
                } else {
                    //bit was 0, set bit to 1 and be done with it
                    nextKey[dimPos] = nextKey[dimPos] | (1L << bitPos);
                    return nextKey;
                }
            }
        }
        return nextKey;
    }

    public static long[] previous(long[] key, int depth) {
        long[] nextKey = Arrays.copyOf(key, key.length);
        for (int bitPos = 0; bitPos < depth; bitPos++) {
            for (int dimPos = key.length - 1; dimPos >= 0; dimPos--) {
                if ((nextKey[dimPos] & (1L << bitPos)) != 0) {
                    //bit was 1, set to 0 and stop
                    nextKey[dimPos] = nextKey[dimPos] & ~(1L << bitPos);
                    return nextKey;
                } else {
                    //bit was 0, set bit to 1 and continue
                    nextKey[dimPos] = nextKey[dimPos] | (1L << bitPos);
                }
            }
        }
        return nextKey;
    }
}
