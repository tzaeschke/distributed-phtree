package ch.ethz.globis.distindex.test;

import ch.ethz.globis.distindex.middleware.PhTreeIndexMiddlewareFactory;
import ch.ethz.globis.distindex.middleware.api.Middleware;
import ch.ethz.globis.distindex.middleware.util.MiddlewareUtil;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class BaseFreeParameterizedTest {

    private static final Logger LOG = LoggerFactory.getLogger(BaseFreeParameterizedTest.class);

    protected static final String HOST = "localhost";
    protected static final int ZK_PORT = 2181;

    private static final int S_BASE_PORT = 7070;

    private static ExecutorService threadPool;
    protected static List<Middleware> middlewares = new ArrayList<>();
    private static TestingServer zkServer;

    private int nrServers;

    private int nrFreeServers;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{2, 2}});
    }

    @BeforeClass
    public static void initExecutor() throws Exception {
        threadPool = Executors.newFixedThreadPool(32);
        try {
            zkServer = new TestingServer(ZK_PORT);
            zkServer.start();
        } catch (Exception e) {
            LOG.warn("Cannot open testing ZK. Attempting to use possible running ZK");
        }
    }

    @AfterClass
    public static void shutdownExecutor() throws InterruptedException, IOException {
        for (Middleware middleware : middlewares) {
            middleware.close();
        }
        threadPool.shutdown();
        if (zkServer != null) {
            zkServer.close();
        }
    }

    public BaseFreeParameterizedTest(int nrServers, int nrFreeServers, boolean init) throws IOException  {
        this.nrServers = nrServers;
        this.nrFreeServers = nrFreeServers;
        if (init || middlewares.size() != nrServers) {
            for (Middleware middleware : middlewares) {
                middleware.close();
            }
            middlewares.clear();

            for (int i = 0; i < nrServers; i++) {
                Middleware current = createMiddleware(HOST, S_BASE_PORT + i * 2, HOST, ZK_PORT);
                current.setJoinedAsFree(false);
                MiddlewareUtil.startMiddleware(threadPool, current);
                middlewares.add(current);
            }

            for (int i = nrServers; i < nrServers + nrFreeServers; i++) {
                Middleware current = createMiddleware(HOST, S_BASE_PORT + i * 2, HOST, ZK_PORT);
                MiddlewareUtil.startMiddleware(threadPool, current);
                current.setJoinedAsFree(true);
                middlewares.add(current);
            }
        }
    }

    protected Middleware createMiddleware(String host, int port, String zkHost, int zkPort) {
        return PhTreeIndexMiddlewareFactory.newPhTree(host, port, zkHost, zkPort);
    }
}
