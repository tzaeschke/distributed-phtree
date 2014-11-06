package ch.ethz.globis.distindex.phtree;

/**
 * Created by bvancea on 06.11.14.
 */

import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.distindex.middleware.api.Middleware;
import ch.ethz.globis.distindex.middleware.net.IndexMiddlewareFactory;
import ch.ethz.globis.distindex.util.MiddlewareUtil;
import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.PhTreeV;
import ch.ethz.globis.pht.PhTreeVProxy;
import ch.ethz.globis.pht.test.util.TestUtilAPI;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Implementation of the TestUtilAPI
 */
public class TestUtilAPIDistributed implements TestUtilAPI {

    /** Current logged */
    private static final Logger LOG = LoggerFactory.getLogger(TestUtilAPIDistributed.class);

    /** The host used for zookeeper */
    private static final String ZK_HOST = "localhost";

    /** The port used for zookeeper */
    private static final int ZK_PORT = 2181;

    /** The base port used when creating index servers */
    private static final int S_BASE_PORT = 7070;

    /** Thread pool used to run the index servers */
    private static ExecutorService threadPool;

    /** References to the index servers */
    private static List<Middleware> middlewares = new ArrayList<>();

    /** Zookeeper server*/
    private static TestingServer zkServer;

    private int nrServers;

    public TestUtilAPIDistributed(int nrServers) throws IOException {
        this.nrServers = nrServers;
    }

    private PHFactory factory = new PHFactory(ZK_HOST, ZK_PORT);

    @Override
    public PhTree newTree(int dim, int depth) {
        return new PhTreeVProxy(newTreeV(dim, depth));
    }

    @Override
    public <T> PhTreeV<T> newTreeV(int dim, int depth) {
        return factory.createPHTreeMap(dim, depth);
    }

    @Override
    public void close(PhTree phTree) {
        //currently not needed
    }

    @Override
    public <T> void close(PhTreeV<T> tPhTreeV) {
        //currently not needed
    }

    @Override
    public void beforeTest() {
        //currently not needed
    }

    @Override
    public void beforeTest(Object[] objects) {
        //currently not needed
    }

    @Override
    public void afterTest() {
        //currently not needed
    }

    @Override
    public void beforeSuite() {
        try {
            threadPool = Executors.newFixedThreadPool(4);
            zkServer = new TestingServer(ZK_PORT);
            zkServer.start();

            for (Middleware middleware : middlewares) {
                middleware.close();
            }
            middlewares.clear();

            for (int i = 0; i < nrServers; i++) {
                Middleware current = IndexMiddlewareFactory.newPhTree(ZK_HOST, S_BASE_PORT + i * 10, ZK_HOST, ZK_PORT);
                MiddlewareUtil.startMiddleware(threadPool, current);
                middlewares.add(current);
            }
        } catch (Exception e) {
            LOG.error("Failed to start test suite. ", e);
        }
    }

    @Override
    public void afterSuite() {
        try {
            for (Middleware middleware : middlewares) {
                middleware.close();
            }
            threadPool.shutdown();
            zkServer.close();
        } catch (Exception e) {
            LOG.error("Exception during suite shutdown. ", e);
        }
    }

    @Override
    public void beforeClass() {
        //currently not needed
    }

    @Override
    public void afterClass() {
        //currently not needed
    }
}