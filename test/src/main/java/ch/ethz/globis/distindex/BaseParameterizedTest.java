package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.middleware.api.Middleware;
import ch.ethz.globis.distindex.middleware.net.IndexMiddlewareFactory;
import ch.ethz.globis.distindex.util.TestUtil;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class BaseParameterizedTest {

    protected static final String HOST = "localhost";
    protected static final int ZK_PORT = 2181;

    private static final int S_BASE_PORT = 7070;


    private static ExecutorService threadPool;
    private static List<Middleware> middlewares = new ArrayList<>();
    private static TestingServer zkServer;

    private int nrServers;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { {1}, {2}});
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

    public BaseParameterizedTest(int nrServers) throws IOException {
        this.nrServers = nrServers;
        for (Middleware middleware : middlewares) {
            middleware.close();
        }
        middlewares.clear();

        for (int i = 0; i < nrServers; i++) {
            Middleware current = IndexMiddlewareFactory.newPhTree(HOST, S_BASE_PORT + i * 10, HOST, ZK_PORT);
            TestUtil.startMiddleware(threadPool, current);
            middlewares.add(current);
        }
    }
}