package ch.ethz.globis.distindex.mapping;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface KeyMapping<K> {

    public Map<String, String> getHosts();

    /**
     * Obtain the hostId of the machine that stores the key received as an argument.
     *
     * @param key
     * @return
     */
    public String getHostId(K key);

    /**
     * Obtain all hostIds that store keys between the range determined by the start and end keys
     * received as arguments.
     * @param start
     * @param end
     * @return
     */
    public List<String> getHostIds(K start, K end);

    /**
     * Get all the host id's which holds keys having the bit prefix received as an argument.
     * @param prefix
     * @return
     */
    public List<String> getHostIds(String prefix);

    /**
     * Obtain all the host ids.
     * @return
     */
    public List<String> getHostIds();

    /**
     * Get the hostId of the host that holds the first key interval.
     * @return
     */
    public String getFirst();

    /**
     * Get the hostUId of the host that holds the next key interval relative to the key interval
     * of the hostId received as an argument.
      * @param hostId
     * @return
     */
    public String getNext(String hostId);

    /**
     * Return all of the host ids that contain the keys received as arguments.
     * @param keys
     * @return
     */
    public Set<String> getHostsContaining(List<K> keys);

    public int getDepth(String hostId);

    /**
     * Add a hostId to the key mapping.
     * @param host
     */
    public void add(String host);
}
