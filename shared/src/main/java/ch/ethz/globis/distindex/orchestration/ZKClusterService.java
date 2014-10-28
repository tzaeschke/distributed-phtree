package ch.ethz.globis.distindex.orchestration;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /** The zookeeper object. */
    private ZooKeeper zk;

    /** The connection string used to connect to Zookeeper. */
    private String hostPort;

    /** The current mapping */
    private KeyMapping<long[]> mapping;

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
                if (isRunning && Event.EventType.NodeChildrenChanged == watchedEvent.getType()) {
                    readCurrentMapping();
                }
            }
        };

        try {
            List<String> children = zk.getChildren(HOSTS_PATH, mappingChangedWatcher);
            String[] hosts = children.toArray(new String[children.size()]);
            mapping = new BSTMapping<>(new LongArrayKeyConverter(), hosts);
        } catch (KeeperException.NoNodeException nne) {
            mapping = new BSTMapping<>(new LongArrayKeyConverter(), new String[] {});
        } catch (KeeperException.ConnectionLossException cle) {
            //retry
            LOG.error("Connection loss exception.", cle);
        } catch (KeeperException.SessionExpiredException se) {
            LOG.error("Session 0x{} is expired.", Long.toHexString(zk.getSessionId()));
        } catch (InterruptedException ie) {
            LOG.error("Connection to Zookeeper interrupted.", ie);
        } catch (KeeperException ke) {
            LOG.error("Error during communication with Zookeeper.", ke);
        }
    }

    @Override
    public void registerHost(String hostId) {
        String zkNodeName = zookeeperNodeName(hostId);
        validateHostId(hostId);

        try {
            if (zk.exists(HOSTS_PATH, false) == null) {
                zk.create(HOSTS_PATH, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            String pathName = zk.create(zkNodeName, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            assert pathName != null;
            assert pathName.equals(zkNodeName);

        } catch (KeeperException.NodeExistsException nee) {
            LOG.warn("Node with name {} already exists on the server. This could happen in a test environment.", zkNodeName);
        } catch (KeeperException ke) {
            LOG.error("Error during communication with Zookeeper.", ke);
        } catch (InterruptedException ie) {
            LOG.error("Connection to Zookeeper interrupted.", ie);
        }
    }

    public void unregisterHost(String hostId) {
        String zkNodeName = zookeeperNodeName(hostId);
        validateHostId(hostId);

        try {
            Stat stat = new Stat();
            zk.getData(zkNodeName, false, stat);

            zk.delete(zkNodeName, stat.getVersion());
        } catch (KeeperException ke) {
            LOG.error("Error during communication with Zookeeper.", ke);
        } catch (InterruptedException ie) {
            LOG.error("Connection to Zookeeper interrupted.", ie);
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
        LOG.info("Cluster service with zk session id 0x{} was disconnected.", Long.toHexString(zk.getSessionId()));
    }

    private void startZK() {
        try {
            zk = new ZooKeeper(hostPort, TIMEOUT, new ZookeeperConnectionWatcher());
        } catch (IOException ioe) {
            LOG.error("Failed to start Zookeeper", ioe);
        }
    }

    private void stopZK() {
        if (zk == null) {
            LOG.error("ClusterService.stopZK() was called when the underlying Zookeeper was not properly initialized");
        }
        try {
            zk.close();
        } catch (InterruptedException ie) {
            LOG.error("Failed to close the Zookeeper client.", ie);
        }
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