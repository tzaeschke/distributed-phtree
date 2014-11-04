package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.pht.*;

public class PHFactory {

    private String zkHost;
    private int zkPort;

    public PHFactory(String zkHost, int zkPort) {
        this.zkHost = zkHost;
        this.zkPort = zkPort;
    }

    public <V> PhTreeV<V> createPHTreeMap(int dim, int depth, Class<V> valueClass) {
        DistributedPHTreeProxy<V> proxy = new DistributedPHTreeProxy<>(zkHost, zkPort, valueClass);
        proxy.create(dim, depth);
        return new DistributedPhTreeV<>(proxy);
    }

    public <V> PhTreeVD<V> createPHTreeVD(int dim, Class<V> valueClass) {
        PhTreeV<V> proxy = createPHTreeMap(dim, Double.SIZE, valueClass);

        return new PhTreeVD<>(proxy);
    }

    public PhTree createPHTreeSet(int dim, int depth) {
        PhTreeV<Object> proxy = createPHTreeMap(dim, depth, Object.class);

        return new PhTreeVProxy(proxy);
    }

    public PhTreeRangeD createPHTreeRangeSet(int dim, int depth) {
        PhTree backingTree = createPHTreeSet(2 * dim, depth);

        return new PhTreeRangeD(backingTree);
    }
}