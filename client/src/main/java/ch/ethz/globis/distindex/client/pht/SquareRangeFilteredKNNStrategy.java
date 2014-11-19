package ch.ethz.globis.distindex.client.pht;

import java.util.List;

/**
 *  After finding the furthest neighbour fn in the hosts holding the key,
 *  find all zones intersecting the square (q - dist(q, fn), q + dist(q, fn))
 *  and perform a range query followed by a filtering based on dist(q, fn) in those areas.
 *
 *  Then apply an additional knn to combine candidates.
 */
public class SquareRangeFilteredKNNStrategy implements KNNStrategy {

    @Override
    public <V> List<long[]> radiusSearch(long[] key, int k, List<long[]> candidates, PHTreeIndexProxy<V> indexProxy) {
        throw new UnsupportedOperationException("Need to implement the filtering on the server side");
    }
}
