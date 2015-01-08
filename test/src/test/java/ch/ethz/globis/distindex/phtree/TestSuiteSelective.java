package ch.ethz.globis.distindex.phtree;

import ch.ethz.globis.distindex.test.TestUtilAPIDistributed;
import ch.ethz.globis.pht.test.*;
import ch.ethz.globis.pht.test.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@RunWith(org.junit.runners.Suite.class)
@Suite.SuiteClasses({TestIndexQueries.class})
public class TestSuiteSelective {

    private static final Logger LOG = LoggerFactory.getLogger(TestPhtreeSuite.class);

    static final int NUMBER_OF_SERVERS = 4;

    @BeforeClass
    public static void init() {
        try {
            TestUtil.setTestUtil(new TestUtilAPIDistributed(NUMBER_OF_SERVERS));
        } catch (IOException e) {
            LOG.error("Failed to create the new testing utility.");
        }
    }
}
