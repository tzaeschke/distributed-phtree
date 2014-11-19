package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.pht.PhDistance;
import ch.ethz.globis.pht.PhDistanceD;

import java.util.List;

/**
 *  After finding the furthest neighbour fn in the hosts holding the key,
 *  find all zones intersecting the square (q - dist(q, fn), q + dist(q, fn))
 *  and perform a range query followed by a filtering based on dist(q, fn) in those areas.
 *
 *  Then apply an additional knn to combine candidates.
 */
public class RangeFilteredKNNStrategy implements KNNStrategy {

    @Override
    public <V> List<long[]> radiusSearch(long[] key, int k, List<long[]> candidates, PHTreeIndexProxy<V> indexProxy) {
        long[] farthestNeighbor = candidates.get(k - 1);
        long distance = MultidimUtil.computeDistance(key, farthestNeighbor);
        long[] start = MultidimUtil.transpose(key, -distance);
        long[] end = MultidimUtil.transpose(key, distance);
        PhDistance measure = new PhDistanceD();
        double dist = measure.dist(key, farthestNeighbor);
        return MultidimUtil.nearestNeighbours(key, k, indexProxy.combineKeys(indexProxy.getRange(start, end, dist)));
    }
}
