package ch.ethz.globis.distindex.test;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import ch.ethz.globis.distindex.mapping.bst.MultidimMapping;
import ch.ethz.globis.distindex.orchestration.ClusterService;

public class SimpleClusterService implements ClusterService<long[]> {

    BSTMapping<long[]> mapping;

    public SimpleClusterService(int bitWidth) {
        this.mapping = new MultidimMapping();
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
    public void connect() {
        //nothing to do here, we are offline
    }

    @Override
    public void disconnect() {
        //nothing to do here, we are offline
    }
}