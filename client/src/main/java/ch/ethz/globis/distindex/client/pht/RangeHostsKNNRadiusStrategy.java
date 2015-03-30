package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.util.MultidimUtil;

import java.util.List;

/**
 * After finding the furthest neighbour fn in the hosts holding the key,
 * find all zones intersecting the square (q - dist(q, fn), q + dist(q, fn)) and perform a knn query into those areas.
 * Then apply an additional knn to combine candidates.
 */
public class RangeHostsKNNRadiusStrategy implements KNNRadiusStrategy {

    /**
     * Perform a radius search to check if there are any neighbours nearer to the query point than the
     * neighbours found on the query host server.
     *
     * This is done using a range search.
     *
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @param candidates                The nearest neighbours on the query point's host server.
     * @return                          The k nearest neighbour points.
     */
    @Override
    public <V> List<long[]> radiusSearch(String initialHost, long[] key, int k, List<long[]> candidates, PHTreeIndexProxy<V> indexProxy) {
        long[] farthestNeighbor = candidates.get(k - 1);
        long distance = MultidimUtil.computeDistance(key, farthestNeighbor);
        long[] start = MultidimUtil.transpose(key, -distance);
        long[] end = MultidimUtil.transpose(key, distance);
        KeyMapping<long[]> mapping = indexProxy.getMapping();

        //make sure to not query the first host twice
        List<String> hostIds = mapping.get(start, end);
        hostIds.remove(initialHost);
        if (hostIds.size() == 0) {
            return candidates;
        }

        List<long[]> expandedCandidates = indexProxy.getNearestNeighbors(hostIds, key, k);

        //add the points we retrieved from the initial call since we didn't query that host again
        expandedCandidates.addAll(candidates);

        return MultidimUtil.nearestNeighbours(key, k, expandedCandidates);
    }
}
