package ch.ethz.globis.distindex.client.pht;

import java.util.List;

public interface KNNRadiusStrategy {

    <V> List<long[]> radiusSearch(String initialHost, long[] key, int k, List<long[]> candidates,
                                  PHTreeIndexProxy<V> indexProxy);
}
