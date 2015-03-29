package ch.ethz.globis.distindex.cluster;

import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.distindex.client.pht.ZKPHFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClusterKNNBenchmark {

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
                tasks.add(new KNNTask(factory, nrEntries, dim, depth));
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
}
