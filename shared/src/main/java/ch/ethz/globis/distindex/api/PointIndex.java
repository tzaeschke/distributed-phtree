package ch.ethz.globis.distindex.api;

import java.util.List;

public interface PointIndex<V> extends Index<long[], V> {

    public List<long[]> getNearestNeighbors(long[] key, int k);
}
