package ch.ethz.globis.distindex.api;

import java.util.List;

/**
 *  Represents a mult-dimensional point index. The key type used by this index is a long array.
 *
 *
 * @param <V>                           The type of the index value.
 */
public interface PointIndex<V> extends Index<long[], V> {

    /**
     * Perform a nearest neighbour search and return the k nearest neighbour's keys.
     *
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @return                          A list consisting of the k nearest keys to the key received as an argument.
     */
    public List<long[]> getNearestNeighbors(long[] key, int k);
}
