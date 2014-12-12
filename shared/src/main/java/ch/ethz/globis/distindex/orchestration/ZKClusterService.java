package ch.ethz.globis.distindex.orchestration;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.mapping.zorder.ZMapping;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ZKClusterService implements ClusterService<long[]> {

    private static final Logger LOG = LoggerFactory.getLogger(ZKClusterService.class);

    /** Timeout for the connection to Zookeeper.*/
    private static final int TIMEOUT = 1000;

    /** The zookeeper object. */
    private CuratorFramework client;

    /** The connection string used to connect to Zookeeper. */
    private String hostPort;

    /** The directory for the mapping.*/
    private static final String MAPPING_PATH = "/mapping";

    /** The current mapping */
    private ZMapping mapping;

    private List<String> hosts = new ArrayList<>();

    /** The directory holding the names of the online servers */
    private static final String SERVERS_PATH = "/servers";

    /**
     * Flag determining if the current service is running or not.
     */
    private boolean isRunning = false;

    public ZKClusterService(String host, int port) {
        this(host + ":" + port);
    }

    public ZKClusterService(String hostPort) {
        this.hostPort = hostPort;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(TIMEOUT, 5);
        client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy);
    }

    @Override
    public void createIndex(Map<String, String> options) {
        int dim = Integer.parseInt(options.get("dim"));
        int depth = Integer.parseInt(options.get("depth"));
        this.mapping = new ZMapping(dim, depth);
        this.mapping.add(getOnlineHosts());
        writeMapping(mapping);
    }

    @Override
    public KeyMapping<long[]> getMapping() {
        return mapping;
    }

    @Override
    public void registerHost(String hostId) {
        ensurePathExists(SERVERS_PATH);
        registerNewHostId(hostId);
    }

    private void registerNewHostId(String hostId) {
        byte[] content = hostId.getBytes();
        String path = SERVERS_PATH + "/" + hostId;
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(path, content);
        } catch (Exception e) {
            LOG.error("Failed to add new server.");
        }
    }

    @Override
    public void unregisterHost(String hostId) {
        mapping.remove(hostId);

        writeMapping(mapping);
    }

    @Override
    public List<String> getOnlineHosts() {
        return hosts;
    }

    @Override
    public void connect() {
        this.client.start();

        ensurePathExists(SERVERS_PATH);
        this.hosts = readCurrentHosts();
        ensurePathExists(MAPPING_PATH);
        this.mapping = readCurrentMapping();
        this.isRunning = true;
    }

    private List<String> readCurrentHosts() {
        List<String> hostIds = new ArrayList<>();
        try {
            hostIds = client.getChildren().usingWatcher(new CuratorWatcher() {
                @Override
                public void process(WatchedEvent watchedEvent) throws Exception {
                    hosts = readCurrentHosts();
                }
            }).forPath(SERVERS_PATH);
        } catch (Exception e) {
            LOG.error("Error reading current mapping: ", e);
        }
        return hostIds;
    }

    @Override
    public void disconnect() {
        isRunning = false;

        CloseableUtils.closeQuietly(client);
    }

    public void writeCurrentMapping() {
        mapping.setVersion(mapping.getVersion() + 1);
        writeMapping(mapping);
    }

    private ZMapping readCurrentMapping() {
        try {
            byte[] data = client.getData().usingWatcher(new CuratorWatcher() {
                @Override
                public void process(WatchedEvent watchedEvent) throws Exception {
                    ZMapping newMapping = readCurrentMapping();
                    if (newMapping == null && mapping != null) {
                        LOG.warn("An attempt was made to overwrite current mapping with a null one.");
                    } else {
                        mapping = newMapping;
                    }
                }
            }).forPath(MAPPING_PATH);
            return ZMapping.deserialize(data);
        } catch (Exception e) {
            LOG.error("Error reading current mapping: ", e);
        }
        return null;
    }

    private void writeMapping(ZMapping mapping) {
        LOG.info("Writing mapping.");
        byte[] data = mapping.serialize();
        ensurePathExists(MAPPING_PATH);

        writeData(MAPPING_PATH, data);
    }

    private void writeData(String path, byte[] data) {
        try {
            client.setData().forPath(path, data);
        } catch (Exception e) {
            LOG.error("Failed to write data {} at path {}", Arrays.toString(data), path);
        }
    }

    private void ensurePathExists(String path) {
        EnsurePath ep = new EnsurePath(path);
        try {
            ep.ensure(client.getZookeeperClient());
        } catch (Exception e) {
            LOG.error("Failed to ensure path {} on {}", MAPPING_PATH, hostPort);
        }
    }
}