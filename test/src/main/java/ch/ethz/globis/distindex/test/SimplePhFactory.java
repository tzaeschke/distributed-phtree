package ch.ethz.globis.distindex.test;

import ch.ethz.globis.distindex.client.pht.DistributedPhTreeV;
import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.pht.*;


public class SimplePhFactory implements PHFactory {

    private ClusterService<long[]> clusterService;

    public SimplePhFactory(ClusterService<long[]> clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public <V> PHTreeIndexProxy<V> createProxy(int dim, int depth) {
        PHTreeIndexProxy<V> proxy = new PHTreeIndexProxy<>(clusterService);
        proxy.create(dim, depth);
        return proxy;
    }

    @Override
    public <V> PhTreeV<V> createPHTreeMap(int dim, int depth) {
        PHTreeIndexProxy<V> proxy = createProxy(dim, depth);
        return new DistributedPhTreeV<>(proxy);
    }

    @Override
    public <V> PhTreeVD<V> createPHTreeVD(int dim) {
        PhTreeV<V> proxy = createPHTreeMap(dim, Double.SIZE);

        return new PhTreeVD<>(proxy);
    }

    @Override
    public PhTree createPHTreeSet(int dim, int depth) {
        PhTreeV<Object> proxy = createPHTreeMap(dim, depth);

        return new PhTreeVProxy(proxy);
    }

    @Override
    public PhTreeRangeD createPHTreeRangeSet(int dim, int depth) {
        PhTree backingTree = createPHTreeSet(2 * dim, depth);

        return new PhTreeRangeD(backingTree);
    }
}
