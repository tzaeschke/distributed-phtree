package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.mapping.bst.BSTNode;
import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.WatchedEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

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
        try (TestingServer zk = newZK(ZK_PORT);
            CuratorFramework client = newClient(ZK_HOST_PORT)) {
            zk.start();
            client.start();

            BSTMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter());
            mapping.add("1");
            mapping.add("2");

            byte[] data = serialize(mapping);
            client.create().forPath(ZK_PATH);
            client.setData().forPath(ZK_PATH, data);
            byte[] serializedBytes = client.getData().forPath(ZK_PATH);

            BSTMapping<long[]> decodedMapping = deserialize(serializedBytes);
            assertEquals(mapping, decodedMapping);

        } catch (Exception e) {
            LOG.error("An exception occurred ", e);
        }
    }

    @Test
    public void testSerialization() {
        BSTMapping<long[]> mapping = new BSTMapping<>(new LongArrayKeyConverter());
        mapping.add("1");
        mapping.add("2");
        byte[] serializedBytes = serialize(mapping);
        BSTMapping<long[]> deserializedMapping = deserialize(serializedBytes);
        assertEquals(mapping, deserializedMapping);
    }

    private void sendToZK(CuratorFramework client) {
    }

    private byte[] serialize(BSTMapping<long[]> mapping) {
        Output output = new Output(new ByteArrayOutputStream());
        getKryo().writeObject(output, mapping);
        return output.getBuffer();
    }

    private BSTMapping<long[]> deserialize(byte[] bytes) {
        return getKryo().readObject(new Input(bytes), BSTMapping.class);
    }

    private Kryo getKryo() {
        if (kryo == null) {
            kryo = new Kryo();
            kryo.register(BSTMapping.class);
            kryo.register(BSTNode.class);
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
