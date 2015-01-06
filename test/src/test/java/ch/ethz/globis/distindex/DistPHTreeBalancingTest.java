package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.middleware.PhTreeRequestHandler;
import ch.ethz.globis.distindex.test.BaseParameterizedTest;
import org.junit.*;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class DistPHTreeBalancingTest extends BaseParameterizedTest {

    private static final Logger LOG = LoggerFactory.getLogger(DistPHTreeBalancingTest.class);

    private PHTreeIndexProxy<String> phTree;

    public DistPHTreeBalancingTest(int nrServers) throws IOException {
        super(nrServers, true);
    }

    @BeforeClass
    public static void changeBalancingParameters() {
        PhTreeRequestHandler.THRESHOLD = 100;
    }

    @AfterClass
    public static void restoredBalancingParameters() {
        PhTreeRequestHandler.THRESHOLD = Integer.MAX_VALUE;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{5}});
    }

    @Before
    public void setupTree() {
        phTree = new PHTreeIndexProxy<>(HOST, ZK_PORT);
    }

    @After
    public void closeTree() throws IOException {
        phTree.close();
    }

    @Test
    public void insertSameHost_AllPositive_1D() throws InterruptedException {
        phTree.create(1, 64);

        int size = 101;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        for (int i = 0; i < size; i++) {
            long[] key = {i};
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }
        LOG.info("Done inserting {} randomly generated entries.", size);

        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }

    @Test
    public void insertSameHost_AllPositive_2D() throws InterruptedException {
        phTree.create(2, 64);

        int size = 101;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        for (int i = 0; i < size; i++) {
            long[] key = {i, i};
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }
        LOG.info("Done inserting {} randomly generated entries.", size);

        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }

    @Test
    public void insertSameHost_AllPositive_Multiple() throws InterruptedException {
        phTree.create(2, 64);

        int size = 201;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        for (int i = 1; i <= size; i++) {
            long[] key = {i, i};
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }

        LOG.info("Done inserting {} randomly generated entries.", size);
        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }

    @Test
    public void insertSameHost_AllNegative() throws InterruptedException {
        phTree.create(2, 64);
        int size = 101;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        for (int i = 1; i <= size; i++) {
            long[] key = {-i, -i};
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }
        LOG.info("Done inserting {} randomly generated entries.", size);
        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }

    @Test
    public void insertSameHost_AllNegative_Multiple() throws InterruptedException {
        phTree.create(2, 64);

        int size = 201;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        for (int i = 1; i <= size; i++) {
            long[] key = {-i, -i};
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }
        LOG.info("Done inserting {} randomly generated entries.", size);
        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }

    @Test
    public void insertHashedWithinCluster() throws InterruptedException {
        phTree.create(2, 64);

        int size = 300;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        long[][] signs = {{1L, 1L}, {1L, -1L}, {-1L, 1L}, {-1L, -1L}};
        for (int i = 0; i < size; i++) {
            long[] key = {i, i};
            for (int j = 0; j < key.length; j++) {
                key[j] = key[j] * signs[i % key.length][j];
            }
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }
        LOG.info("Done inserting {} randomly generated entries.", size);
        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }
}