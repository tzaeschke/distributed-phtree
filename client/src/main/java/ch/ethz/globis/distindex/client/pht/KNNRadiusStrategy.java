package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.distindex.mapping.bst.MultidimMapping;

import java.util.List;

public interface KNNRadiusStrategy {

    <V> List<long[]> radiusSearch(long[] key, int k, List<long[]> candidates,
                                  MultidimMapping mapping,
                                  BSTMappingKNNStrategy<V> knnStrategy,
                                  PHTreeIndexProxy<V> indexProxy);
}
