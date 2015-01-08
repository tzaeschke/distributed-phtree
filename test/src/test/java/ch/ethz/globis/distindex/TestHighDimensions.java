package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.test.BaseParameterizedTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

public class TestHighDimensions extends BaseParameterizedTest {

    private PHTreeIndexProxy<Integer> phTree;

    public TestHighDimensions(int nrServers) throws IOException {
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

    @Test
    public void testDim_20() {
        int dim = 100;
        int depth = 64;
        phTree.create(dim, depth);
        assertEquals(0, phTree.size());
    }
}