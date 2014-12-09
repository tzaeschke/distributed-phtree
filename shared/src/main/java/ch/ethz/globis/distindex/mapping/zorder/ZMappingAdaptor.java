package ch.ethz.globis.distindex.mapping.zorder;

import ch.ethz.globis.distindex.mapping.KeyMapping;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by bvancea on 08.12.14.
 */
public class ZMappingAdaptor implements KeyMapping<long[]> {

    private ZMapping mapping;

    public ZMappingAdaptor(ZMapping mapping) {
        this.mapping = mapping;
    }

    @Override
    public Map<String, String> asMap() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getHostId(long[] key) {
        return mapping.get(key);
    }

    @Override
    public List<String> getHostIds(long[] start, long[] end) {
        return mapping.get(start, end);
    }

    @Override
    public List<String> getHostIds() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getFirst() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getNext(String hostId) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Set<String> getHostsContaining(List<long[]> keys) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int getDepth(String hostId) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void add(String host) {
        mapping.add(host);
    }

    @Override
    public void remove(String host) {
        mapping.remove(host);
    }

    @Override
    public void split(String splittingHostId, String receiverHostId, int sizeMoved) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setSize(String host, int size) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getHostForSplitting(String currentHostId) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String getLargestZone(String currentHostId) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
