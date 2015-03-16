package ch.ethz.globis.distindex.cluster;

import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.distindex.client.pht.ZKPHFactory;
import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.pht.PhTree;
import org.lwjgl.Sys;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClusterInsertionBenchmark {

    private static String ZK_HOST = "localhost";
    private static int ZK_PORT = 2181;

    public static void main(String[] args) {
        extractZKInfo(args);

        PHFactory factory = new ZKPHFactory(ZK_HOST, ZK_PORT);
        int dim = 2;
        int depth = 64;
        int nrEntries = 10000;

        insertWithClients(factory, nrEntries);
    }

    private static void insertWithClients(PHFactory factory, int nrEntries) {
        int nrClients = 4;
        ExecutorService pool = Executors.newFixedThreadPool(4);

        List<Runnable> tasks = new ArrayList<Runnable>();
        try {
            for (int i = 0; i < nrClients; i++) {
                tasks.add(new InsertionTask(factory, nrEntries));
            }
            for (Runnable task : tasks) {
                pool.execute(task);
            }

            pool.shutdown();
            pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void insert(PhTree tree, int nrEntries) {
        List<long[]> entries = new ArrayList<long[]>();
        Random random = new Random();
        for (int i = 0; i < nrEntries; i++) {
            entries.add(new long[] { random.nextLong(), random.nextLong() });
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

    public static class InsertionTask implements Runnable {

        private PhTree tree;
        private int nrEntries;

        public InsertionTask(PHFactory factory, int nrEntries) {
            this.tree = factory.createPHTreeSet(2, 64);
            this.nrEntries = nrEntries;
        }

        @Override
        public void run() {
            insert(tree, nrEntries);
        }
    }
}

