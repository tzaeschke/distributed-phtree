package ch.ethz.globis.distindex.test;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import ch.ethz.globis.distindex.mapping.bst.MultidimMapping;
import ch.ethz.globis.distindex.orchestration.ClusterService;

import java.util.List;
import java.util.Map;

public class SimpleClusterService implements ClusterService<long[]> {

    BSTMapping<long[]> mapping;

    public SimpleClusterService(int bitWidth) {
        this.mapping = new MultidimMapping();
    }

    @Override
    public void createIndex(Map<String, String> options) {
        //nothing to do here, we are offline
    }

    @Override
    public KeyMapping<long[]> getMapping() {
        return mapping;
    }

    @Override
    public void registerHost(String hostId) {
        mapping.add(hostId);
    }

    @Override
    public void unregisterHost(String hostId) {
        throw new UnsupportedOperationException("Is this even necessary?");
    }

    @Override
    public List<String> getOnlineHosts() {
        return mapping.get();
    }

    @Override
    public void connect() {
        //nothing to do here, we are offline
    }

    @Override
    public void disconnect() {
        //nothing to do here, we are offline
    }
}