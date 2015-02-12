package ch.ethz.globis.distindex.client.pht;

import java.util.List;

public interface KNNStrategy<V> {

    public List<long[]> getNearestNeighbors(long[] key, int k, PHTreeIndexProxy<V> indexProxy);

    public void setRadiusStrategy(KNNRadiusStrategy radiusStrategy);
}
