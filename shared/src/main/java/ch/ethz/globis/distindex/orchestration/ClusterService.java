package ch.ethz.globis.distindex.orchestration;

import ch.ethz.globis.distindex.mapping.KeyMapping;

import java.util.List;
import java.util.Map;

public interface ClusterService<K> {

    /**
     * Connect to the cluster.
     */
    public void connect();

    /**
     * Disconnect from the cluster.
     */
    public void disconnect();

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
     * Register the host as a free host. This host will not be part of the cluster until a host
     * that is full will pick it.
     *
     * @param hostId                    The id of the host to be marked as free.
     */
    public void registerFreeHost(String hostId);

    /**
     * Remove a free host from the list of free hosts and return it to the list of free hosts.
     *
     * @return                          The id of the removed host.
     */
    public String getNextFreeHost();

    /**
     * @return                              The online hosts registered in the cluster.
     */
    public List<String> getOnlineHosts();

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
     * Returns an approximation of the number of entries stored by a host. This is the latest value that
     * the client read from Zookeeper.
     *
     * @param hostId                        The id of the host.
     * @return                              The number of entries of a host.
     */
    public int getSize(String hostId);

    /**
     * Change the number of entries associated with a host in ZooKeeper.
     * @param hostId
     * @param size
     */
    public void setSize(String hostId, int size);
}