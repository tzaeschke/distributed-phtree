package ch.ethz.globis.distindex.orchestration;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.mapping.bst.MultidimMapping;
import com.esotericsoftware.kryo.Kryo;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Implementation of the cluster service using Zookeeper.
 */
public class BSTMapClusterService implements ClusterService<long[]> {

    private static final Logger LOG = LoggerFactory.getLogger(BSTMapClusterService.class);

    /** Timeout for the connection to Zookeeper.*/
    private static final int TIMEOUT = 1000;

    /** The directory for the peers.*/
    private static final String HOSTS_PATH = "/peers";

    private static final String INDEX_PATH = "/index";

    private static final String MAPPING_PATH = "/mapping";

    /** The zookeeper object. */
    private CuratorFramework client;

    /** The connection string used to connect to Zookeeper. */
    private String hostPort;

    /** The current mapping */
    private MultidimMapping mapping;

    private boolean isRunning = false;

    public BSTMapClusterService(String host, int port) {
        this.hostPort = host + ":" + port;
    }

    public BSTMapClusterService(String hostPort) {
        this.hostPort = hostPort;
    }

    @Override
    public void createIndex(Map<String, String> options) {
        //no need to do anything here
    }

    public KeyMapping<long[]> getMapping() {
        return mapping;
    }

    private CuratorWatcher mappingChangedWatcher() {
        return new CuratorWatcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (isRunning && (Watcher.Event.EventType.NodeChildrenChanged == watchedEvent.getType()
                        || Watcher.Event.EventType.NodeDataChanged == watchedEvent.getType())) {
                    readCurrentMapping();
                }
            }
        };
    }
    public void readCurrentMapping() {


        try {
            if (client.checkExists().forPath(MAPPING_PATH) == null) {
                mapping = new MultidimMapping();
            } else {
                byte[] data = client.getData().usingWatcher(mappingChangedWatcher()).forPath(MAPPING_PATH);
                mapping = MultidimMapping.deserialize(data);
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
            writeCurrentMapping(mapping);
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

    private void writeCurrentMapping(MultidimMapping mapping) throws Exception {
        byte[] data = mapping.serialize();
        if (client.checkExists().forPath(MAPPING_PATH) == null) {
            client.create().forPath(MAPPING_PATH);
        }
        client.setData().forPath(MAPPING_PATH, data);
        client.checkExists().usingWatcher(mappingChangedWatcher()).forPath(MAPPING_PATH);
    }

    public void unregisterHost(String hostId) {
        validateHostId(hostId);

        try {
            mapping.remove(hostId);
            writeCurrentMapping(mapping);

        } catch (KeeperException ke) {
            LOG.error("Error during communication with Zookeeper.", ke);
        } catch (InterruptedException ie) {
            LOG.error("Connection to Zookeeper interrupted.", ie);
        } catch (Exception e) {
            LOG.error("Exception occurred", e);
        }
    }

    @Override
    public List<String> getOnlineHosts() {
        return mapping.get();
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
        try {
            LOG.info("Cluster service with zk session id 0x{} was disconnected.",
                    Long.toHexString(client.getZookeeperClient().getZooKeeper().getSessionId()));
            stopZK();
        } catch (Exception e) {
            LOG.error("Failed to obtain a ZK handle");
        }
    }

    @Override
    public void writeCurrentMapping() {
        try {
            writeCurrentMapping(mapping);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startZK() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(TIMEOUT, 3);
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

    private String zookeeperNodeName(String hostId) {
        return HOSTS_PATH + "/" + hostId;
    }
}