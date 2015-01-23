package ch.ethz.globis.distindex.orchestration;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.mapping.zorder.ZMapping;
import org.apache.curator.CuratorConnectionLossException;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryLoop;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class ZKClusterService implements ClusterService<long[]> {

    private static final Logger LOG = LoggerFactory.getLogger(ZKClusterService.class);

    /** Timeout for the connection to Zookeeper.*/
    private static final int TIMEOUT = 1000;

    /** The zookeeper object. */
    private CuratorFramework client;

    /** The connection string used to connect to Zookeeper. */
    private String hostPort;

    /** The directory for the mapping. */
    private static final String MAPPING_PATH = "/mapping";

    /** The directory for the sizes. */
    private static final String SIZE_PATH = "/sizes";

    /** The current mapping. */
    private ZMapping mapping;

    /** The list of online hosts. */
    private List<String> hosts = new ArrayList<>();

    /** The directory holding the names of the online servers. */
    private static final String SERVERS_PATH = "/servers";

    /** The directory where the free servers are registered */
    private static final String FREE_PATH = "/free";

    /** Contains the sizes of each host. */
    private Map<String, Integer> sizes = new HashMap<>();

    public ZKClusterService(String host, int port) {
        this(host + ":" + port);
    }

    public ZKClusterService(String hostPort) {
        this.hostPort = hostPort;

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(TIMEOUT, 3);
        client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy);
    }

    @Override
    public void createIndex(Map<String, String> options) {
        int dim = Integer.parseInt(options.get("dim"));
        int depth = Integer.parseInt(options.get("depth"));
        this.mapping = new ZMapping(dim, depth);
        this.mapping.add(getOnlineHosts());

        LOG.info("Writing mapping with version {} at create.", mapping.getVersion());
        writeCurrentMapping();
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

            initSizeCounter(hostId);
        } catch (Exception e) {
            LOG.error("Failed to add new server.");
        }
    }

    private void initSizeCounter(String hostId) throws Exception {
        if (sizes.containsKey(hostId)) {
            throw new IllegalStateException("Host size should not be initialized");
        }
        int value = 0;
        sizes.put(hostId, value);
        String path = sizePath(hostId);
        ensurePathExists(path);
        client.setData().forPath(path, intToByte(value));
    }

    private byte[] intToByte(int value) {
        return ByteBuffer.allocate(Integer.SIZE).putInt(value).array();
    }

    private int byteToInt(byte[] array) {
        return ByteBuffer.wrap(array).getInt();
    }

    @Override
    public void deregisterHost(String hostId) {
        mapping.remove(hostId);

        LOG.info("Writing mapping with version {} on de-registration ", mapping.getVersion());
        writeCurrentMapping();
    }

    @Override
    public List<String> getOnlineHosts() {
        return hosts;
    }

    @Override
    public void connect() {
        initResources();

        this.hosts = readCurrentHosts();
        this.mapping = readCurrentMapping();
    }

    @Override
    public void disconnect() {
        closeResources();
    }

    private void writeCurrentMapping() {
        writeMapping(mapping);
    }

    @Override
    public int getSize(String hostId) {
        if (!sizes.containsKey(hostId)) {
            readSize(hostId);
        }
        return sizes.get(hostId);
    }

    @Override
    public void setSize(String hostId, int size) {
        if (!sizes.containsKey(hostId)) {
            try {
                initSizeCounter(hostId);
            } catch (Exception e) {
                LOG.error("Failed to initialize size counter.");
            }
        }
        String path = sizePath(hostId);
        try {
            ensurePathExists(path);
            client.setData().forPath(path, intToByte(size));
            sizes.put(hostId, size);
        } catch (Exception e) {
            LOG.error("Could not update path {} with value {}.", path, size);
        }
    }

    @Override
    public int setIntervalEnd(String hostId, long[] key, String freeHostId) {
        if (freeHostId != null) {
            addToHostsRight(hostId, freeHostId);
        }
        //FIXME maybe there will be some inconsistency if updateTree is not called before writing
        boolean writeFailed = true;
        Stat stat = new Stat();
        while (writeFailed) {
            this.mapping = readCurrentMapping(stat);
            this.mapping.changeIntervalEnd(hostId, key, freeHostId);
            this.mapping.setVersion(this.mapping.getVersion() + 1);
            try {
                int version = stat.getVersion();
                if (null != client.setData().withVersion(version).forPath(MAPPING_PATH, mapping.serialize())) {
                    writeFailed = false;
                }
            } catch (Exception e) {
                LOG.error("Failed to get stat for path {}", MAPPING_PATH);
            }
        }
        return mapping.getVersion();
    }

    private void addToHostsRight(String hostId, String newHostId) {
        int index = Collections.binarySearch(hosts, hostId);
        this.hosts.add(index + 1, newHostId);
        registerNewHostId(newHostId);
    }

    @Override
    public void registerFreeHost(String hostId) {
        String path = FREE_PATH + "/" + hostId;
        try {
            client.create().forPath(path);
        } catch (Exception e) {
            LOG.error("Failed to write host to free list.", e);
        }
    }

    @Override
    public String getNextFreeHost() {
        String freeHostId = null;
        Stat freePathStat = new Stat();
        try {
            List<String> children = client.getChildren().storingStatIn(freePathStat).forPath(FREE_PATH);
            if (children.size() > 0) {
                if (removeChild(FREE_PATH, children.get(0), freePathStat)) {
                    freeHostId = children.get(0);
                }
            }
        } catch (Exception e) {
            LOG.error("Error ocurred when trying to obtain the children of path {}", FREE_PATH);
        }

        return freeHostId;
    }

    private boolean removeChild(String parentPath, String childId, Stat parentStat) {
        int parentVersion = parentStat.getVersion();
        boolean removed = false;
        String childPath = parentPath + "/" + childId;
        try {
            client.inTransaction()
                    .check().withVersion(parentVersion).forPath(parentPath)
                    .and()
                    .delete().forPath(childPath)
                    .and()
                    .commit();
            removed = true;
        } catch (Exception e) {
            LOG.info("Free host with path {} was already removed.", childId);
        }
        return removed;
    }

    private void readSize(final String hostId) {
        String path = sizePath(hostId);

        try {
            byte[] data = client.getData().usingWatcher(new CuratorWatcher() {
                @Override
                public void process(WatchedEvent watchedEvent) throws Exception {
                    readSize(hostId);
                }
            }).forPath(path);
            this.sizes.put(hostId, byteToInt(data));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String sizePath(String hostId) {
        return SIZE_PATH + "/" + hostId;
    }

    private void initResources() {
        try {
            this.client.start();
            ensurePathExists(SIZE_PATH);
            ensurePathExists(SERVERS_PATH);
            ensurePathExists(MAPPING_PATH);
            ensurePathExists(FREE_PATH);
        } catch (Exception e) {
            LOG.error("Error initializing resource.", e);
        }
    }

    private void closeResources() {
        CloseableUtils.closeQuietly(client);
    }

    private ZMapping readCurrentMapping() {
        return readCurrentMapping(new Stat());
    }

    private ZMapping readCurrentMapping(Stat stat) {
        ZMapping zMap = null;
        try {
            byte[] data = client.getData().storingStatIn(stat).usingWatcher(new CuratorWatcher() {
                @Override
                public void process(WatchedEvent watchedEvent) throws Exception {
                    ZMapping newMapping = readCurrentMapping();
                    if (newMapping == null && mapping != null) {
                        LOG.warn("An attempt was made to overwrite current mapping with a null one.");
                    } else {
                        mapping = newMapping;
                    }
                    LOG.debug("Host {} just updated its mapping to version {}", hostPort, (mapping != null) ? mapping.getVersion() : -1);
                }
            }).forPath(MAPPING_PATH);
            zMap = ZMapping.deserialize(data);
        } catch (Exception e) {
            LOG.error("Error reading current mapping: ", e);
        }
        return zMap;
    }

    private List<String> readCurrentHosts() {
        List<String> hostIds = new ArrayList<>();
        try {
            CuratorFrameworkState state = client.getState();
            if (CuratorFrameworkState.STARTED.equals(state)) {
                hostIds = client.getChildren().usingWatcher(new CuratorWatcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) throws Exception {
                        hosts = readCurrentHosts();
                    }
                }).forPath(SERVERS_PATH);
            } else {
                LOG.warn("Attempting to read current hosts when the Zookeeper client is in state {}", state);
            }
        } catch (Exception e) {
            LOG.error("Error reading current mapping: ", e);
        }
        return hostIds;
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
        try {
            if (client.checkExists().forPath(path) == null) {
                String result = client.create().forPath(path, new byte[0]);
            }
        } catch (Exception e) {
            LOG.error("Problem ensuring that the path exists", e);
        }
    }
}