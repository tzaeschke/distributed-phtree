package ch.ethz.globis.distindex.orchestration;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.mapping.bst.BSTNode;
import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Implementation of the cluster service using Zookeeper.
 */
public class ZKClusterService implements ClusterService<long[]> {

    private static final Logger LOG = LoggerFactory.getLogger(ZKClusterService.class);

    /** Timeout for the connection to Zookeeper.*/
    private static final int TIMEOUT = 5000;

    /** The directory for the peers.*/
    private static final String HOSTS_PATH = "/peers";

    private static final String INDEX_PATH = "/index";

    private static final String MAPPING_PATH = "/mapping";

    /** The zookeeper object. */
    private CuratorFramework client;

    /** The connection string used to connect to Zookeeper. */
    private String hostPort;

    /** The current mapping */
    private KeyMapping<long[]> mapping;

    private static Kryo kryo;

    private boolean isRunning = false;

    public ZKClusterService(String host, int port) {
        this.hostPort = host + ":" + port;
    }

    public ZKClusterService(String hostPort) {
        this.hostPort = hostPort;
    }

    public KeyMapping<long[]> getMapping() {
        return mapping;
    }

    public void readCurrentMapping() {
        Watcher mappingChangedWatcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (isRunning && (Event.EventType.NodeChildrenChanged == watchedEvent.getType()
                        || Event.EventType.NodeDataChanged == watchedEvent.getType())) {
                    readCurrentMapping();
                }
            }
        };

        int bitWidth = 64;
        try {
            if (client.checkExists().forPath(INDEX_PATH) == null) {
                client.create().forPath(INDEX_PATH);
                client.setData().forPath(INDEX_PATH, String.valueOf(bitWidth).getBytes());
            } else {
                String bitWidthString = new String(client.getData().usingWatcher(mappingChangedWatcher).forPath(INDEX_PATH));
                bitWidth = Integer.parseInt(bitWidthString);
            }
            if (client.checkExists().forPath(MAPPING_PATH) == null) {
                mapping = new BSTMapping<>(new LongArrayKeyConverter(bitWidth));
            } else {
                byte[] data = client.getData().usingWatcher(mappingChangedWatcher).forPath(MAPPING_PATH);
                mapping = deserialize(data);
            }

        } catch (KeeperException.ConnectionLossException cle) {
            //retry
            LOG.error("Connection loss exception.", cle);
        } catch (KeeperException.SessionExpiredException se) {
            try {
                LOG.error("Session 0x{} is expired.", Long.toHexString(client.getZookeeperClient().getZooKeeper().getSessionId()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (InterruptedException ie) {
            LOG.error("Connection to Zookeeper interrupted.", ie);
        } catch (KeeperException ke) {
            LOG.error("Error during communication with Zookeeper.", ke);
        } catch (Exception e) {
            LOG.error("Error occured", e);
        }
    }

    @Override
    public void registerHost(String hostId) {
        String zkNodeName = zookeeperNodeName(hostId);
        validateHostId(hostId);
        mapping.add(hostId);

        try {
            if (client.checkExists().forPath(HOSTS_PATH) == null) {
                client.create().withMode(CreateMode.PERSISTENT).forPath(HOSTS_PATH,"".getBytes());
            }
            String pathName = client.create().withMode(CreateMode.EPHEMERAL).forPath(zkNodeName);

            writeCurrentMapping();

            assert pathName != null;
            assert pathName.equals(zkNodeName);
        } catch (KeeperException.NodeExistsException nee) {
            LOG.warn("Node with name {} already exists on the server. This could happen in a test environment.", zkNodeName);
        } catch (KeeperException ke) {
            LOG.error("Error during communication with Zookeeper.", ke);
        } catch (InterruptedException ie) {
            LOG.error("Connection to Zookeeper interrupted.", ie);
        } catch (Exception e) {
            LOG.error("Error communicating with ZK");
        }
    }

    private void writeCurrentMapping() throws Exception {
        byte[] data = serialize(mapping);
        if (client.checkExists().forPath(MAPPING_PATH) == null) {
            client.create().forPath(MAPPING_PATH);
        }
        client.setData().forPath(MAPPING_PATH, data);
    }

    public void unregisterHost(String hostId) {
        String zkNodeName = zookeeperNodeName(hostId);
        validateHostId(hostId);

        try {
            Stat stat = client.checkExists().forPath(zkNodeName);
            client.delete().withVersion(stat.getVersion()).forPath(zkNodeName);

            List<String> hosts = client.getChildren().forPath(HOSTS_PATH);
            mapping = new BSTMapping<>(new LongArrayKeyConverter(64), hosts.toArray(new String[hosts.size()]));
            writeCurrentMapping();

        } catch (KeeperException ke) {
            LOG.error("Error during communication with Zookeeper.", ke);
        } catch (InterruptedException ie) {
            LOG.error("Connection to Zookeeper interrupted.", ie);
        } catch (Exception e) {
            LOG.error("Exception occurred", e);
        }
    }

    @Override
    public void connect() {
        isRunning = true;

        startZK();

        readCurrentMapping();
    }

    @Override
    public void disconnect() {
        isRunning = false;
        stopZK();
        try {
            LOG.info("Cluster service with zk session id 0x{} was disconnected.",
                    Long.toHexString(client.getZookeeperClient().getZooKeeper().getSessionId()));
        } catch (Exception e) {
            LOG.error("Failed to obtain a ZK handle");
        }
    }

    private void startZK() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy);
        client.start();
    }

    private void stopZK() {
        if (client == null) {
            LOG.error("ClusterService.stopZK() was called when the underlying Zookeeper was not properly initialized");
        }
        client.close();
    }

    private void validateHostId(String hostId) {
        if ((hostId == null) || hostId.equals("")) {
            throw new IllegalArgumentException("Host id invalid: " + hostId);
        }
    }

    private byte[] serialize(KeyMapping<long[]> mapping) {
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
            kryo.register(KeyMapping.class);
        }
        return kryo;
    }

    private String zookeeperNodeName(String hostId) {
        return HOSTS_PATH + "/" + hostId;
    }
}