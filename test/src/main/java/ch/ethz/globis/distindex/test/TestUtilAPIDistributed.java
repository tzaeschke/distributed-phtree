package ch.ethz.globis.distindex.test;

import ch.ethz.globis.distindex.client.pht.DistributedPhTreeV;
import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.client.pht.ZKPHFactory;
import ch.ethz.globis.distindex.middleware.api.Middleware;
import ch.ethz.globis.distindex.middleware.PhTreeIndexMiddlewareFactory;
import ch.ethz.globis.distindex.middleware.util.MiddlewareUtil;
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
    private List<DistributedPhTreeV> trees = new ArrayList<>();

    private int nrServers;

    public TestUtilAPIDistributed(int nrServers) throws IOException {
        this.nrServers = nrServers;
        beforeSuite();
    }

    private ZKPHFactory factory = new ZKPHFactory(ZK_HOST, ZK_PORT);

    @Override
    public PhTree newTree(int dim, int depth) {
        return new PhTreeVProxy(newTreeV(dim, depth));
    }

    @Override
    public <T> PhTreeV<T> newTreeV(int dim, int depth) {

        PhTreeV<T> tree = factory.createPHTreeMap(dim, depth);
        trees.add((DistributedPhTreeV) tree);
        return tree;
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
        System.out.println("Clearing trees.");
        try {
            for (DistributedPhTreeV tree : trees) {
                tree.getProxy().close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            trees.clear();
        }
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
            threadPool = Executors.newFixedThreadPool(32);
            startZK();

            for (Middleware middleware : middlewares) {
                middleware.close();
            }
            middlewares.clear();

            for (int i = 0; i < nrServers; i++) {
                Middleware current = PhTreeIndexMiddlewareFactory.newPhTree(ZK_HOST, S_BASE_PORT + i * 10, ZK_HOST, ZK_PORT);
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
            stopZK();
        } catch (Exception e) {
            LOG.error("Exception during suite shutdown. ", e);
        }
    }

    private void startZK() {
        try {
            zkServer = new TestingServer(ZK_PORT);
            zkServer.start();
        } catch (Exception e) {
            LOG.warn("Cannot open testing ZK. Attempting to use possible running ZK");
        }
    }

    private void stopZK() {
        try {
            zkServer.stop();
        } catch (NullPointerException npe) {
            LOG.error("ZK was not initialized.");
        } catch (IOException e) {
            LOG.error("Failed to close ZK.", e);
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