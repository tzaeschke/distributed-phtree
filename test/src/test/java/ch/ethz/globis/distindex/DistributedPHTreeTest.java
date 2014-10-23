package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.client.pht.DistributedPHTree;
import ch.ethz.globis.distindex.middleware.net.IndexMiddlewareFactory;
import ch.ethz.globis.distindex.middleware.api.Middleware;
import org.apache.curator.test.TestingServer;
import org.junit.*;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class DistributedPHTreeTest {

    private static final int ZK_PORT = 2181;
    private static ExecutorService threadPool;

    @Before
    public void initExecutor() {
        threadPool = Executors.newFixedThreadPool(2);
    }

    @After
    public void shutdownExecutor() throws InterruptedException {
        threadPool.shutdownNow();
        threadPool.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testPutAndGet2D() throws Exception {
        int dim = 2;
        int depth = 64;
        String host = "localhost";

        try (TestingServer zkServer = new TestingServer(ZK_PORT);
             Middleware middleware = IndexMiddlewareFactory.newPHTreeMiddleware(7070);
             Middleware second = IndexMiddlewareFactory.newPHTreeMiddleware(7080)
        ) {
            zkServer.start();
            startMiddleware(middleware);
            startMiddleware(second);

            DistributedPHTree<String> phTree = new DistributedPHTree<>(host, ZK_PORT, String.class);
            phTree.create(dim, depth);

            long[] key = new long[]{1L, 2L};
            String value = "hello";
            phTree.put(key, value);
            String retrieved = phTree.get(key);
            assertEquals("Wrong value retrieval", value, retrieved);

            key = new long[]{-1L, -2L};
            value = "bye";
            phTree.put(key, value);
            retrieved = phTree.get(key);
            assertEquals("Wrong value retrieval", value, retrieved);
            phTree.close();
        }
    }

    @Test
    public void testPutAndGet3D() throws Exception {
        int dim = 3;
        int depth = 64;
        String host = "localhost";

        try (TestingServer zkServer = new TestingServer(ZK_PORT);
            Middleware middleware = IndexMiddlewareFactory.newPHTreeMiddleware(7070)) {
            zkServer.start();
            startMiddleware(middleware);
            DistributedPHTree<BigInteger> phTree = new DistributedPHTree<>(host, ZK_PORT, BigInteger.class);
            phTree.create(dim, depth);

            int nrEntries = 100000;
            Random random = new Random();

            long[] key;
            BigInteger value;
            for (int i = 0; i < nrEntries; i++) {
                key = new long[]{random.nextLong(), random.nextLong(), random.nextLong()};
                value = new BigInteger(50, random);
                phTree.put(key, value);
                assertEquals("Value does not match with value retrieved from the tree.", value, phTree.get(key));
            }

            phTree.close();
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