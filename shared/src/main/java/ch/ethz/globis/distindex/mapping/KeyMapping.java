package ch.ethz.globis.distindex.mapping;

import java.util.List;
import java.util.Map;

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
     * Add a hostId to the key mapping.
     * @param host
     */
    public void add(String host);
}
