package ch.ethz.globis.distindex.cluster;

import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.distindex.client.pht.ZKPHFactory;
import ch.ethz.globis.pht.PhTree;
import org.lwjgl.Sys;

import java.sql.Time;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClusterInsertionBenchmark {

    private static String ZK_HOST = "localhost";
    private static int ZK_PORT = 2181;
    private static int NR_ENTRIES = 50000;
    private static int NR_THREADS = 4;

    public static void main(String[] args) {
        extractArguments(args);

        PHFactory factory = new ZKPHFactory(ZK_HOST, ZK_PORT);
        int dim = 2;
        int depth = 64;
        int nrEntries = NR_ENTRIES;

        insertWithClients(factory, nrEntries, dim, depth);
    }

    private static void insertWithClients(PHFactory factory, int nrEntries, int dim, int depth) {
        int nrClients = NR_THREADS;
        ExecutorService pool = Executors.newFixedThreadPool(nrClients);

        List<Runnable> tasks = new ArrayList<Runnable>();
        try {
            for (int i = 0; i < nrClients; i++) {
                tasks.add(new InsertionTask(factory, nrEntries, dim, depth));
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

    private static void extractArguments(String[] args) {
        if (args.length > 0) {
            ZK_HOST = args[0];
        }
        if (args.length > 1) {
            ZK_PORT = Integer.valueOf(args[1]);
        }
        if (args.length > 2) {
            NR_ENTRIES = Integer.valueOf(args[2]);
        }
        if (args.length > 3) {
            NR_THREADS = Integer.valueOf(args[3]);
        }
    }

    public static class InsertionTask implements Runnable {

        private PhTree tree;
        private int nrEntries;

        public InsertionTask(PHFactory factory, int nrEntries, int dim, int depth) {
            this.tree = factory.createPHTreeSet(dim, depth);
            this.nrEntries = nrEntries;
        }

        @Override
        public void run() {
            insert(tree, nrEntries);
        }

        private void insert(PhTree tree, int nrEntries) {
            List<long[]> entries = new ArrayList<long[]>();
            Random random = new Random();
            for (int i = 0; i < nrEntries; i++) {
                entries.add(new long[]{
                        gaussianRandomValue(random), gaussianRandomValue(random)
                });
            }

            doInsert(tree, entries);
        }

        private long gaussianRandomValue(Random random) {
            double r = random.nextGaussian();
            return (long) ((Long.MAX_VALUE - 1) * r);
        }

        private void doInsert(PhTree tree, List<long[]> points) {
            DateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            long startTime = System.currentTimeMillis();
            //System.out.println("Inserting " + points.size() + " points. " + startTime);
            long start, end;
            for (long[] point : points) {
                start = System.currentTimeMillis();
                tree.insert(point);
                end = System.currentTimeMillis();
                System.out.println(date.format(new Date()) + ",end,insert,"+ (end - start));
            }
            long endTime = System.currentTimeMillis();

            double duration = (endTime - startTime) / 1000.0;
            double throughput = points.size() / duration;
//            System.out.println("Tree size: " + tree.size() + " " + endTime);
//            System.out.println("Throughput is : " + throughput);
        }
    }
}

