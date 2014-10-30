package ch.ethz.globis.distindex.util;

import ch.ethz.globis.pht.PhTree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultidimUtil {

    public static List<long[]> nearestNeighbours(long[] q, int k, List<long[]> points) {
        PhTree tree = createTree(points);
        return tree.nearestNeighbour(k, q);
    }

    public static List<long[]> sort(List<long[]> points) {
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
}