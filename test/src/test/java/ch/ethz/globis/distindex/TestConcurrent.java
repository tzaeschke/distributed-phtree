package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.middleware.PhTreeRequestHandler;
import ch.ethz.globis.distindex.test.BaseParameterizedTest;
import org.junit.*;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestConcurrent extends BaseParameterizedTest {

    private PHTreeIndexProxy<Integer> phTree;

    public TestConcurrent(int nrServers) throws IOException {
        super(nrServers, true);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{4}});
    }

    @Before
    public void setupTree() {
        phTree = new PHTreeIndexProxy<>(HOST, ZK_PORT);
    }

    @After
    public void closeTree() throws IOException {
        phTree.close();
    }

    @BeforeClass
    public static void changeBalancingParameters() {
        PhTreeRequestHandler.THRESHOLD = 100;
    }

    @AfterClass
    public static void restoredBalancingParameters() {
        PhTreeRequestHandler.THRESHOLD = Integer.MAX_VALUE;
    }

    @Test
    public void testConcurrentInserts() {
        int dim = 3;
        int depth = 64;
        phTree.create(dim, depth);
        ExecutorService pool = Executors.newFixedThreadPool(2);

        int entries = 300;
        long[][] keyPositive = new long[entries][dim], keysNegative = new long[entries][dim];
        for (int i = 1; i < entries; i++) {
            for (int j = 0; j < dim; j++) {
                keyPositive[i][j] = i;
                keysNegative[i][j] = -i;
            }
        }
        ThreadedInserter pos = new ThreadedInserter(keyPositive, phTree);
        ThreadedInserter neg = new ThreadedInserter(keysNegative, phTree);
        try {
            pool.execute(pos);
            pool.execute(neg);
            pool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            pool.shutdown();
        }
    }

    class ThreadedInserter implements Runnable {

        long[][] keys;
        PHTreeIndexProxy<Integer> phTree;

        ThreadedInserter(long[][] keys, PHTreeIndexProxy<Integer> phTree) {
            this.keys = keys;
            this.phTree = phTree;
        }

        @Override
        public void run() {
            for (long[] key: keys) {
                phTree.put(key, null);
            }
        }
    }

}
