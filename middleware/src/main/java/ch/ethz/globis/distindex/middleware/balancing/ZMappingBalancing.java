package ch.ethz.globis.distindex.middleware.balancing;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.middleware.IndexContext;

import java.util.List;

public class ZMappingBalancing implements BalancingStrategy {

    /** The in-memory index context */
    private IndexContext indexContext;

    public ZMappingBalancing(IndexContext indexContext) {
        this.indexContext = indexContext;
    }

    @Override
    public void balance() {
        List<String> hosts = indexContext.getClusterService().getOnlineHosts();
        KeyMapping<long[]> mapping = indexContext.getClusterService().getMapping();

        System.out.println("Sizes");
        for (String host : hosts) {
            System.out.println(host + ": " + mapping.getSize(host));
        }
    }
}
