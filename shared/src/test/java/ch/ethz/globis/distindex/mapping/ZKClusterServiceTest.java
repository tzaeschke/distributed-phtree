package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.orchestration.BSTMapClusterService;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;
import org.apache.curator.test.TestingServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

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
            clusterService.deregisterHost("1");
            clusterService.deregisterHost("2");
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
    public void testFreeHosts() {
        ClusterService<long[]> freeHost1 = null, freeHost2 = null, client = null;
        final String freeHostId1 = "one";
        final String freeHostId2 = "two";
        List<String> hostIds = new ArrayList<String>() {{ add(freeHostId1);add(freeHostId2); }};
        try (TestingServer zk = newZK(ZK_PORT)) {
            zk.start();
            freeHost1 = new ZKClusterService(ZK_HOST, ZK_PORT);
            freeHost2 = new ZKClusterService(ZK_HOST, ZK_PORT);
            client = new ZKClusterService(ZK_HOST, ZK_PORT);
            startClusterServices(freeHost1, freeHost2, client);
            freeHost1.registerFreeHost(freeHostId1);
            freeHost2.registerFreeHost(freeHostId2);

            String freeHostId = client.getNextFreeHost();
            assertNotNull(freeHostId);
            assertTrue(hostIds.contains(freeHostId));
            hostIds.remove(freeHostId);

            freeHostId = client.getNextFreeHost();
            assertNotNull(freeHostId);
            assertTrue(hostIds.contains(freeHostId));
            hostIds.remove(freeHostId);

            freeHostId = client.getNextFreeHost();
            assertNull(freeHostId);


        } catch (Exception e) {
            LOG.error("An exception occurred ", e);
        } finally {
            closeClusterServices(freeHost1, freeHost2, client);
        }
    }

    @Test
    public void testRegisterHost() {
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

            writer.deregisterHost("one");
            onlineHosts = reader.getOnlineHosts();
            assertNotNull(onlineHosts);
            assertEquals(1, onlineHosts.size());

            writer.deregisterHost("two");
            onlineHosts = reader.getOnlineHosts();
            assertNotNull(onlineHosts);
            assertEquals(0, onlineHosts.size());
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

    private void startClusterServices(ClusterService<long[]>... clusterServices) {
        for (ClusterService<long[]> clusterService :  clusterServices) {
            clusterService.connect();
        }
    }

    private void closeClusterServices(ClusterService<long[]>... clusterServices) {
        for (ClusterService<long[]> clusterService :  clusterServices) {
            clusterService.disconnect();
        }
    }

}
