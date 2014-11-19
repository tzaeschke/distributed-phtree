package ch.ethz.globis.distindex.client.pht;

import java.util.List;

public interface KNNStrategy {

    <V> List<long[]> radiusSearch(long[] key, int k, List<long[]> candidates, PHTreeIndexProxy<V> indexProxy);
}
