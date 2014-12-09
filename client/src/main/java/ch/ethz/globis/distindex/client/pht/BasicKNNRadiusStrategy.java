package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.distindex.mapping.ZCurveHelper;
import ch.ethz.globis.distindex.mapping.bst.MultidimMapping;
import ch.ethz.globis.distindex.util.MultidimUtil;

import java.util.List;
import java.util.Set;

/**
 * After finding the furthest neighbour fn in the hosts holding the key, look into all neighbouring ares and check for
 * neighbours that are closer to q than fn.
 */
public class BasicKNNRadiusStrategy implements KNNRadiusStrategy {

    /**
     * Perform a radius search to check if there are any neighbours nearer to the query point than the
     * neighbours found on the query host server.
     *
     * This is done by locating the furthest neighbour from the candidates.
     *
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @param candidates                The nearest neighbours on the query point's host server.
     * @return                          The k nearest neighbour points.
     */
    @Override
    public <V> List<long[]> radiusSearch(long[] key, int k, List<long[]> candidates, MultidimMapping mapping,
                                         BSTMappingKNNStrategy<V> knnStrategy,
                                         PHTreeIndexProxy<V> indexProxy) {

        long[] farthestNeighbor = candidates.get(k - 1);
        List<long[]> neighbors = ZCurveHelper.getProjectedNeighbours(key, farthestNeighbor);
        Set<String> neighbourHosts = mapping.getHostsContaining(neighbors);

        List<long[]> nearestNeighbors = knnStrategy.getNearestNeighbors(neighbourHosts, key, k, indexProxy);
        return MultidimUtil.nearestNeighbours(key, k, nearestNeighbors);
    }
}
