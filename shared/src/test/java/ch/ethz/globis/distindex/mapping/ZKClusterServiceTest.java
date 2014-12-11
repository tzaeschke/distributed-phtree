package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.orchestration.BSTMapClusterService;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;
import org.apache.curator.test.TestingServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ZKClusterServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ZKClusterServiceTest.class);
    private static final int ZK_PORT = 2181;
    private static final String ZK_HOST = "localhost";

    @Test
    public void testAddHost() {
        ClusterService<long[]> clusterService = null;
        try (TestingServer zk = newZK(ZK_PORT)) {
            zk.start();
            clusterService  = new ZKClusterService(ZK_HOST, ZK_PORT);
            clusterService.connect();
            clusterService.registerHost("1");
            clusterService.registerHost("2");
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

    @Test
    public void testHosts() {
        ClusterService<long[]> reader = null, writer = null;
        try (TestingServer zk = newZK(ZK_PORT)) {
            zk.start();
            writer = new ZKClusterService(ZK_HOST, ZK_PORT);
            writer.connect();
            reader = new ZKClusterService(ZK_HOST, ZK_PORT);
            reader.connect();

            writer.registerHost("one");
            writer.registerHost("two");
            Thread.sleep(10);
            List<String> onlineHosts = reader.getOnlineHosts();
            assertNotNull(onlineHosts);
            assertEquals(2, onlineHosts.size());
        }  catch (Exception e) {
            LOG.error("An exception occurred ", e);
            if (reader != null) {
                reader.disconnect();
            }
            if (writer != null) {
                writer.disconnect();
            }
        }
    }

    private TestingServer newZK(int port) throws Exception {
        TestingServer zk = new TestingServer(port);
        return zk;
    }
}
