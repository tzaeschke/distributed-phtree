package ch.ethz.globis.distindex.mapping;

import java.util.List;

public interface KeyMapping<K> {

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
}
