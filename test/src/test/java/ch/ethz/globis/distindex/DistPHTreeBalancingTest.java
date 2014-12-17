package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.test.BaseParameterizedTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class DistPHTreeBalancingTest extends BaseParameterizedTest {

    private PHTreeIndexProxy<String> phTree;

    public DistPHTreeBalancingTest(int nrServers) throws IOException {
        super(nrServers);
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

    @Test
    public void insertSameHost() throws InterruptedException {
        phTree.create(2, 64);

        int size = 101;
        IndexEntryList<long[], String> entries = new IndexEntryList<>();
        for (int i = 0; i < size; i++) {
            long[] key = {i, i};
            phTree.put(key, Arrays.toString(key));
            entries.add(key, Arrays.toString(key));
        }
        Thread.sleep(2000);
        for (IndexEntry<long[], String> entry :  entries) {
            String retrieved = phTree.get(entry.getKey());
            assertEquals(entry.getValue(), retrieved);
        }
    }
}