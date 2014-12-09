package ch.ethz.globis.distindex.mapping;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface KeyMapping<K> {

    /**
     * Add a hostId to the key mapping.
     * @param host
     */
    public void add(String host);

    /**
     * Remove a hostId from the keyMapping
     * @param host
     */
    public void remove(String host);

    /**
     * Set the number of keys associated with a host.
     *
     * @param host
     * @param size
     */
    public void setSize(String host, int size);

    /**
     * Obtain the hostId of the machine that stores the key received as an argument.
     *
     * @param key
     * @return
     */
    public String get(K key);

    /**
     * Obtain all hostIds that store keys between the range determined by the start and end keys
     * received as arguments.
     * @param start
     * @param end
     * @return
     */
    public List<String> get(K start, K end);

    /**
     * Obtain all the host ids.
     * @return
     */
    public List<String> get();

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
     * Return the number of hosts within the mapping.
     * @return
     */
    public int size();

    /**
     * Clear the mapping.
     */
    public void clear();
}