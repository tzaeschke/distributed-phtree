package ch.ethz.globis.distindex.mapping;

import java.util.*;

/**
 * This class is a mock and is only used during testing.
 *
 * Might want to even remove it.
 * @param <K>
 */
public class NonDistributedMapping<K> implements KeyMapping<K> {

    private final String host;
    private final List<String> hostList;

    public NonDistributedMapping(String host) {
        this.host = host;
        this.hostList = new ArrayList<>();
        hostList.add(host);
    }

    @Override
    public Map<String, String> getHosts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getHostId(K key) {
        return host;
    }

    @Override
    public List<String> getHostIds(K start, K end) {
        return hostList;
    }

    @Override
    public List<String> getHostIds() {
        return hostList;
    }

    @Override
    public String getFirst() {
        return hostList.get(0);
    }

    @Override
    public String getNext(String hostId) {
        int index = Collections.binarySearch(hostList, hostId) + 1;
        if (index >= hostList.size()) {
            return null;
        } else {
            return hostList.get(index + 1);
        }
    }

    @Override
    public Set<String> getHostsContaining(List<K> keys) {
        return new HashSet<>(hostList);
    }

    @Override
    public int getDepth(String hostId) {
        return 0;
    }

    @Override
    public void add(String hostId) {
        int index = Collections.binarySearch(hostList, hostId);
        hostList.add(index, hostId);
    }
}