package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.distindex.util.MultidimUtil;

import java.util.List;

/**
 * After finding the furthest neighbour fn in the hosts holding the key,
 * find all zones intersecting the square (q - dist(q, fn), q + dist(q, fn))
 * and perform a range query in those areas. Then apply an additional knn to combine candidates.
 */
public class RangeKNNStrategy implements KNNStrategy {

    /**
     * Perform a radius search to check if there are any neighbours nearer to the query point than the
     * neighbours found on the query host server.
     *
     * This is done using a knn search on hosts intersecting the range (q - r, q + r) .
     *
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @param candidates                The nearest neighbours on the query point's host server.
     * @return                          The k nearest neighbour points.
     */
    @Override
    public <V> List<long[]> radiusSearch(long[] key, int k, List<long[]> candidates, PHTreeIndexProxy<V> indexProxy) {
        long[] farthestNeighbor = candidates.get(k - 1);
        long distance = indexProxy.computeDistance(key, farthestNeighbor);
        long[] start = indexProxy.transpose(key, -distance);
        long[] end = indexProxy.transpose(key, distance);
//        List<String> hostIds = indexProxy.getMapping().getHostIds(start, end);
//        List<long[]> expandedCandidates = indexProxy.getNearestNeighbors(hostIds, key, k);
        return MultidimUtil.nearestNeighbours(key, k, indexProxy.combineKeys(indexProxy.getRange(start, end)));
    }
}
