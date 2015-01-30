package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.pht.PhTreeV;
import ch.ethz.globis.pht.v3.PhTree3;
import ch.ethz.globis.pht.v5.PhTree5;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The in-memory context associated with the index.
 */
public class IndexContext {

    /** The current in-memory tree. */
    private PhTreeV<byte[]> tree;

    /** The cluster service, connected to the ZooKeeper cluster. */
    private ClusterService<long[]> clusterService;

    /** The host information on which the middleware is running. */
    private final String host;

    /** The port on which the middleware is running. */
    private final int port;

    /** The hostId of the current middleware, currently host:port */
    private final String hostId;

    /** The version of the last balancing. */
    private int lastBalancingVersion = 0;

    private AtomicBoolean isBalancing = new AtomicBoolean(false);

    public IndexContext(String host, int port) {
        this.host = host;
        this.port = port;
        this.hostId = host + ":" + port;
    }

    public void initTree(int dim, int depth) {
        this.tree = new PhTree5<>(dim, depth);
    }


    public PhTreeV<byte[]> getTree() {
        return tree;
    }

    public ClusterService<long[]> getClusterService() {
        return clusterService;
    }

    public void setClusterService(ClusterService<long[]> clusterService) {
        this.clusterService = clusterService;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getHostId() {
        return hostId;
    }

    public int getLastBalancingVersion() {
        return lastBalancingVersion;
    }

    public void setLastBalancingVersion(int lastBalancingVersion) {
        this.lastBalancingVersion = lastBalancingVersion;
    }

    public boolean canStartBalancing() {
        return isBalancing.compareAndSet(false, true);
    }

    public boolean endBalancing() {
        return isBalancing.compareAndSet(true, false);
    }

    public boolean isBalancing() {
        return isBalancing.get();
    }
}