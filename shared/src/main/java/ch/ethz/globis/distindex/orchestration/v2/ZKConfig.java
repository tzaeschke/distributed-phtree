package ch.ethz.globis.distindex.orchestration.v2;

import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ZKConfig {

    private static final Logger LOG = LoggerFactory.getLogger(ZKConfig.class);
    private static final String RW_LOCK_PATH = "/rwlock";
    private static final String HOSTS_CACHE_PATH = "/hosts";
    private static final String SIZE_CACHE_PATH = "/size";

    private String hostPort;
    private CuratorFramework client;
    private TreeCache treeCache;
    private TreeCache sizeCache;
    private InterProcessReadWriteLock rwLock;

    private Map<String, HostInfo> hosts = new HashMap<>();
    private Map<String, Integer> sizes = new HashMap<>();

    public ZKConfig(String hostPort) {
        this.hostPort = hostPort;
        this.client = CuratorFrameworkFactory.newClient(hostPort, new ExponentialBackoffRetry(1000, 3));
        this.treeCache = new TreeCache(client, HOSTS_CACHE_PATH);
        this.treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                LOG.info("Tree cache event: " + treeCacheEvent);
            }
        });

        this.sizeCache = new TreeCache(client, SIZE_CACHE_PATH);
        this.sizeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                LOG.info("Size cache event: " + treeCacheEvent);
            }
        });
        this.rwLock = new InterProcessReadWriteLock(client, RW_LOCK_PATH);
    }

    public void add(String hostPort) {
        String sizePath = SIZE_CACHE_PATH + "/" + hostPort;
        String treePath = HOSTS_CACHE_PATH + "/" + hostPort;

        String largest = largestHost();
        HostInfo newHostInfo = new HostInfo();
        throw new UnsupportedOperationException();
    }

    private String largestHost() {
        int size = -1;
        String largest = null;
        for (Map.Entry<String, Integer> entry : sizes.entrySet()) {
            if (entry.getValue() > size) {
                largest = entry.getKey();
            }
        }
        return largest;
    }

    public void start() {
        this.client.start();

        try {
            this.treeCache.start();
            this.sizeCache.start();
        } catch (Exception e) {
            LOG.error("Failed to start the tree cache.");
        }
    }

    public void close() {
        CloseableUtils.closeQuietly(treeCache);
        CloseableUtils.closeQuietly(sizeCache);
        CloseableUtils.closeQuietly(client);
    }
}