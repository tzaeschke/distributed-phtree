package ch.ethz.globis.distindex.client;

import ch.ethz.globis.distindex.mapping.KeyMapping;

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

    public Map<String, String> asMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String get(K key) {
        return host;
    }

    @Override
    public List<String> get(K start, K end) {
        return hostList;
    }

    public List<String> getHostIds(String prefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> get() {
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

    public Set<String> getHostsContaining(List<K> keys) {
        return new HashSet<>(hostList);
    }
    public int getDepth(String hostId) {
        return 0;
    }

    @Override
    public void add(String hostId) {
        int index = Collections.binarySearch(hostList, hostId);
        hostList.add(index, hostId);
    }

    @Override
    public void remove(String hostId) {
        int index = Collections.binarySearch(hostList, hostId);
        hostList.remove(index);
    }

    public void split(String splittingHostId, String receiverHostId, int sizeMoved) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSize(String host, int size) {
        //do nothing
    }

    @Override
    public int getSize(String host) {
        return 0;
    }

    public String getHostForSplitting(String currentHostId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return hostList.size();
    }

    @Override
    public void clear() {
    }

    public String getLargestZone(String currentHostId) {
        return null;
    }
}