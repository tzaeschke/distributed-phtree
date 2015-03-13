package ch.ethz.globis.distindex.cluster;

import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.distindex.client.pht.ZKPHFactory;
import ch.ethz.globis.pht.PhTree;
import org.lwjgl.Sys;

import java.util.ArrayList;
import java.util.List;

public class ClusterInsertionBenchmark {

    private static String ZK_HOST = "localhost";
    private static int ZK_PORT = 2181;

    public static void main(String[] args) {
        extractZKInfo(args);

        PHFactory factory = new ZKPHFactory(ZK_HOST, ZK_PORT);
        int dim = 2;
        int depth = 64;
        PhTree tree = factory.createPHTreeSet(dim, depth);

        int nrEntries = 100000;

        insert(tree, nrEntries);

    }

    private static void insert(PhTree tree, int nrEntries) {
        List<long[]> entries = new ArrayList<long[]>();
        for (int i = 0; i < nrEntries; i++) {
            entries.add(new long[] { i, i });
        }

        doInsert(tree, entries);
    }

    private static void doInsert(PhTree tree, List<long[]> points) {
        System.out.println("Inserting " + points.size() + " points.");

        long startTime = System.currentTimeMillis();

        for (long[] point : points) {
            tree.insert(point);
        }
        long endTime = System.currentTimeMillis();

        double duration = (endTime - startTime) / 1000.0;
        double throughput = points.size() / duration;
        System.out.println("Tree size: " + tree.size());
        System.out.println("Throughput is : " + throughput);
    }


    private static void extractZKInfo(String[] args) {
        if (args.length != 2) {
            return;
        }
        ZK_HOST = args[0];
        ZK_PORT = Integer.valueOf(args[1]);
    }
}