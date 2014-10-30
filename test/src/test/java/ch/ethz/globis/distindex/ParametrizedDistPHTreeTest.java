package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.client.pht.DistributedPHTree;
import ch.ethz.globis.distindex.middleware.api.Middleware;
import ch.ethz.globis.distindex.middleware.net.IndexMiddleware;
import ch.ethz.globis.distindex.middleware.net.IndexMiddlewareFactory;
import ch.ethz.globis.distindex.util.TestUtil;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ParametrizedDistPHTreeTest {

    private static final String host = "localhost";
    private static final int S_BASE_PORT = 7070;
    private static final int ZK_PORT = 2181;

    private static ExecutorService threadPool;
    private static List<Middleware> middlewares = new ArrayList<>();
    private static TestingServer zkServer;

    private DistributedPHTree<String> tree;

    private int nrServers;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { {1}, {2}, {4}});
    }

    @BeforeClass
    public static void initExecutor() throws Exception {
        threadPool = Executors.newFixedThreadPool(4);
        zkServer = new TestingServer(ZK_PORT);
        zkServer.start();

    }

    @AfterClass
    public static void shutdownExecutor() throws InterruptedException, IOException {
        for (Middleware middleware : middlewares) {
            middleware.close();
        }
        threadPool.shutdown();
        zkServer.close();
    }

    @Before
    public void setupTree() {
        tree = new DistributedPHTree<>(host, ZK_PORT, String.class);
        tree.create(2, 64);
    }

    @Test
    public void testInsert() {
        int nrEntries = 100000;
        Random random = new Random();

        long[] key;
        String value;
        for (int i = 0; i < nrEntries; i++) {
            key = new long[]{random.nextLong(), random.nextLong()};
            value = new BigInteger(50, random).toString();
            tree.put(key, value);
            assertEquals("Value does not match with value retrieved from the tree.", value, tree.get(key));
        }
    }

    public ParametrizedDistPHTreeTest(int nrServers) throws IOException {
        this.nrServers = nrServers;
        for (Middleware middleware : middlewares) {
            middleware.close();
        }
        middlewares.clear();

        for (int i = 0; i < nrServers; i++) {
            Middleware current = IndexMiddlewareFactory.newPhTree(host, S_BASE_PORT + i * 10, host, ZK_PORT);
            TestUtil.startMiddleware(threadPool, current);
            middlewares.add(current);
        }
    }
}