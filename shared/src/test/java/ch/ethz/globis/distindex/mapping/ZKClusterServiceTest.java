package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.mapping.bst.BSTNode;
import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.math.stat.clustering.Cluster;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;

public class ZKClusterServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ZKClusterServiceTest.class);

    private static final int ZK_PORT = 2181;
    private static final String ZK_HOST = "localhost";
    private static final String ZK_HOST_PORT = ZK_HOST + ":" + ZK_PORT;
    private static final String ZK_PATH = "/mapping";

    private static Kryo kryo;

    @Test
    public void testAddHost() {
        ClusterService<long[]> clusterService = null;
        try (TestingServer zk = newZK(ZK_PORT)) {
            zk.start();
            clusterService  = new ZKClusterService(ZK_HOST, ZK_PORT);
            clusterService.connect();
            clusterService.registerHost("1");
            clusterService.registerHost("2");
            KeyMapping<long[]> mapping1 = clusterService.getMapping();
            clusterService.disconnect();

            clusterService = new ZKClusterService(ZK_HOST, ZK_PORT);
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
    public void testSerialization() {
        BSTMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter());
        mapping.add("1");
        mapping.add("2");
        byte[] serializedBytes = serialize(mapping);
        BSTMapping<long[]> deserializedMapping = (BSTMapping<long[]>) deserialize(serializedBytes);
        assertEquals(mapping, deserializedMapping);
    }
    private byte[] serialize(KeyMapping<long[]> mapping) {
        Output output = new Output(new ByteArrayOutputStream());
        getKryo().writeObject(output, mapping);
        return output.getBuffer();
    }

    private KeyMapping<long[]> deserialize(byte[] bytes) {
        return getKryo().readObject(new Input(bytes), BSTMapping.class);
    }

    private Kryo getKryo() {
        if (kryo == null) {
            kryo = new Kryo();
            kryo.register(KeyMapping.class);
        }
        return kryo;
    }

    private TestingServer newZK(int port) throws Exception {
        TestingServer zk = new TestingServer(port);
        return zk;
    }

    private CuratorFramework newClient(String connectionString) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        return client;
    }
}
