package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.client.pht.DistributedPHTree;
import ch.ethz.globis.distindex.middleware.IndexMiddlewareFactory;
import ch.ethz.globis.distindex.middleware.api.Middleware;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

public class DistributedPHTreeTest {

    private static final int PORT = 7070;
    private static ExecutorService threadPool;

    @BeforeClass
    public static void initExecutor() {
        threadPool = Executors.newSingleThreadExecutor();
    }

    @AfterClass
    public static void shutdownExecutor() {
        threadPool.shutdown();
    }

    @Test
    public void testPutAndGet2D() {
        Middleware middleware = null;
        try {
            int dim = 2;
            int depth = 64;
            String host = "localhost";
            middleware = IndexMiddlewareFactory.newPHTreeMiddleware(PORT, dim, depth, String.class);
            startMiddleware(middleware);

            DistributedPHTree<String> phTree = new DistributedPHTree<>(host, PORT, String.class);

            long[] key = new long[]{1L, 2L};
            String value = "hello";
            phTree.put(key, value);
            String retrieved = phTree.get(key);
            assertEquals("Wrong value retrieval", value, retrieved);

        } finally {
            if (middleware != null) {
                middleware.shutdown();
            }
        }
    }

    @Test
    public void testPutAndGet3D() {
        Middleware middleware = null;

        try {
            int dim = 3;
            int depth = 64;
            String host = "localhost";
            middleware = IndexMiddlewareFactory.newPHTreeMiddleware(PORT, dim, depth, BigInteger.class);
            startMiddleware(middleware);
            DistributedPHTree<BigInteger> phTree = new DistributedPHTree<>(host, PORT, BigInteger.class);

            int nrEntries = 100000;
            Random random = new Random();

            long[] key;
            BigInteger value;
            for (int i = 0; i < nrEntries; i++) {
                key = new long[]{random.nextLong(), random.nextLong(), random.nextLong()};
                value = new BigInteger(50, random);
                phTree.put(key, value);
                try {
                    assertEquals("Value does not match with value retrieved from the tree.", value, phTree.get(key));
                } catch (Exception e) {}
            }

        } finally {
            if (middleware != null) {
                middleware.shutdown();
            }
        }
    }

    public void startMiddleware(Middleware middleware) {
        threadPool.execute((Runnable) middleware);
        while (!middleware.isRunning()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                System.err.format("Failed to sleep while initializing middleware.");
                e.printStackTrace();
            }
        }
    }
}