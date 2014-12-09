package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.orchestration.BSTMapClusterService;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import org.apache.curator.test.TestingServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class BSTMapClusterServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(BSTMapClusterServiceTest.class);
    private static final int ZK_PORT = 2181;
    private static final String ZK_HOST = "localhost";

    @Test
    public void testAddHost() {
        ClusterService<long[]> clusterService = null;
        try (TestingServer zk = newZK(ZK_PORT)) {
            zk.start();
            clusterService  = new BSTMapClusterService(ZK_HOST, ZK_PORT);
            clusterService.connect();
            clusterService.registerHost("1");
            clusterService.registerHost("2");
            KeyMapping<long[]> mapping1 = clusterService.getMapping();
            clusterService.disconnect();

            clusterService = new BSTMapClusterService(ZK_HOST, ZK_PORT);
            clusterService.connect();
            KeyMapping<long[]> mapping2 = clusterService.getMapping();
            clusterService.unregisterHost("1");
            clusterService.unregisterHost("2");
            assertEquals(0, mapping2.size());
            clusterService.disconnect();
        } catch (Exception e) {
            LOG.error("An exception occurred ", e);
        } finally {
            if (clusterService != null) {
                clusterService.disconnect();
            }
        }
    }

    private TestingServer newZK(int port) throws Exception {
        TestingServer zk = new TestingServer(port);
        return zk;
    }
}
