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

    public void writeCurrentMapping();

    public int getSize(String hostId);

    public void setSize(String hostId, int size);

    public void lockForReading();

    public void releaseAfterReading();

    public void lockForWriting();

    public void releaseAfterWriting();
}