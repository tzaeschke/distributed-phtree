package ch.ethz.globis.distindex.orchestration;

import ch.ethz.globis.distindex.mapping.KeyMapping;

import java.util.List;
import java.util.Map;

public interface ClusterService<K> {

    /**
     * Handle the creation of a new index.
     *
     * @param options
     */
    public void createIndex(Map<String, String> options);

    /**
     * Reads the current key-to-machine mapping from the distributed cluster configuration.
     * @return                              The current cluster configuration.
     */
    public KeyMapping<K> getMapping();

    /**
     * Register the host with the hostId as an index peer.
     * @param hostId
     */
    public void registerHost(String hostId);

    /**
     * Register the host with the hostId as an index peer.
     * @param hostId
     */
    public void deregisterHost(String hostId);

    /**
     * @return                              The online hosts registered in the cluster.
     */
    public List<String> getOnlineHosts();

    /**
     * Connect to the cluster.
     */
    public void connect();

    /**
     * Disconnect from the cluster.
     */
    public void disconnect();

    public int getSize(String hostId);

    public void setSize(String hostId, int size);

    public int setIntervalEnd(String hostId, long[] key, String freeHostId);

    /**
     * Register the host as a free host. This host will not be part of the cluster until a host
     * that is full will pick it.
     *
     * @param hostId                    The id of the host to be marked as free.
     */
    public void registerFreeHost(String hostId);

    public String getNextFreeHost();
}