package ch.ethz.globis.distindex.orchestration;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.mapping.zorder.ZMapping;
import ch.ethz.globis.distindex.mapping.zorder.ZMappingAdaptor;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.EnsurePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class ZKCClusterService implements ClusterService {

    private static final Logger LOG = LoggerFactory.getLogger(ZKCClusterService.class);

    /** Timeout for the connection to Zookeeper.*/
    private static final int TIMEOUT = 1000;

    /** The zookeeper object. */
    private CuratorFramework client;

    /** The connection string used to connect to Zookeeper. */
    private String hostPort;

    /** The directory for the mapping.*/
    private static final String MAPPING_PATH = "/mapping";

    /** The node cache associated with the mapping node */
    private NodeCache nodeCache;

    /** The current mapping */
    private ZMapping mapping;

    /**
     * Flag determining if the current service is running or not.
     */
    private boolean isRunning = false;

    public ZKCClusterService(String host, int port) {
        this(host + ":" + port);
    }

    public ZKCClusterService(String hostPort) {
        this.hostPort = hostPort;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(TIMEOUT, 3);
        client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy);

        nodeCache = new NodeCache(client, MAPPING_PATH);
    }

    @Override
    public KeyMapping getMapping() {
        return new ZMappingAdaptor(mapping);
    }

    @Override
    public void registerHost(String hostId) {
        mapping.add(hostId);
        writeMapping(mapping);
    }

    @Override
    public void unregisterHost(String hostId) {
        mapping.remove(hostId);

        writeMapping(mapping);
    }

    @Override
    public void connect() {
        client.start();

        initNodeCache(nodeCache);

        mapping = readCurrentMapping();

        isRunning = true;
    }

    private void initNodeCache(NodeCache nodeCache) {
        try {
            nodeCache.start();
            NodeCacheListener listener = new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    mapping = readCurrentMapping();
                }
            };

            nodeCache.getListenable().addListener(listener);
        } catch (Exception e) {
            LOG.error("Failed to start the node cache.");
        }
    }

    @Override
    public void disconnect() {
        isRunning = false;

        CloseableUtils.closeQuietly(nodeCache);
        CloseableUtils.closeQuietly(client);
    }

    private ZMapping readCurrentMapping() {
        ChildData nodeData = nodeCache.getCurrentData();
        if (nodeData == null) {
            return new ZMapping();
        }
        byte[] data = nodeData.getData();
        return ZMapping.deserialize(data);
    }

    private void writeMapping(ZMapping mapping) {
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