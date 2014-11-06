package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.pht.*;

public class PHFactory {

    private String zkHost;
    private int zkPort;

    public PHFactory(String zkHost, int zkPort) {
        this.zkHost = zkHost;
        this.zkPort = zkPort;
    }

    public <V> PhTreeV<V> createPHTreeMap(int dim, int depth) {
        DistributedPHTreeProxy<V> proxy = new DistributedPHTreeProxy<>(zkHost, zkPort);
        proxy.create(dim, depth);
        return new DistributedPhTreeV<>(proxy);
    }

    public <V> PhTreeVD<V> createPHTreeVD(int dim) {
        PhTreeV<V> proxy = createPHTreeMap(dim, Double.SIZE);

        return new PhTreeVD<>(proxy);
    }

    public PhTree createPHTreeSet(int dim, int depth) {
        PhTreeV<Object> proxy = createPHTreeMap(dim, depth);

        return new PhTreeVProxy(proxy);
    }

    public PhTreeRangeD createPHTreeRangeSet(int dim, int depth) {
        PhTree backingTree = createPHTreeSet(2 * dim, depth);

        return new PhTreeRangeD(backingTree);
    }
}