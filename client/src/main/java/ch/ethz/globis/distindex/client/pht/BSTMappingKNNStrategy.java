package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.distindex.mapping.ZCurveHelper;
import ch.ethz.globis.distindex.mapping.bst.MultidimMapping;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BSTMappingKNNStrategy<V> implements KNNStrategy<V> {

    /** The logger used by this class. */
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(BSTMappingKNNStrategy.class);

    private KNNRadiusStrategy radiusStrategy = new RangeKNNRadiusStrategy();

    @Override
    public List<long[]> getNearestNeighbors(long[] key, int k, PHTreeIndexProxy<V> indexProxy) {
        LOG.debug("KNN request started for key={} and k={}", Arrays.toString(key), k);
        MultidimMapping keyMapping = (MultidimMapping) indexProxy.getMapping();
        String keyHostId = keyMapping.get(key);
        List<long[]> candidates = indexProxy.getNearestNeighbors(keyHostId, key, k);
        List<long[]> neighbours;
        if (candidates.size() < k) {
            neighbours = iterativeExpansion(keyMapping, key, k, indexProxy);
        } else {
            neighbours = radiusSearch(key, k, candidates, indexProxy);
        }
        LOG.debug("KNN request ended for key={} and k={}", Arrays.toString(key), k);
        return neighbours;
    }

    /**
     * Perform an iterative expansion search for the nearest neighbour. This should be called if
     * the host containing the query point does not contain K nearest neighbours.
     *
     * @param key                       The query point.
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     */
    <V> List<long[]> iterativeExpansion(MultidimMapping keyMapping, long[] key, int k, PHTreeIndexProxy<V> indexProxy) {
        int dim = indexProxy.getDim();
        int depth = indexProxy.getDepth();

        String hostId = keyMapping.get(key);

        List<long[]> candidates;
        int regionBitWidth = depth - keyMapping.getDepth(hostId) / dim;
        Set<String> currentHostIds;
        boolean foundK = false;
        int hops = 1;
        do {
            if (hops + regionBitWidth > depth) {
                currentHostIds = new HashSet<>(keyMapping.get());
            } else {
                List<long[]> projections = ZCurveHelper.getProjectionsWithinHops(key, hops, regionBitWidth);
                currentHostIds = keyMapping.getHostsContaining(projections);
            }
            candidates = indexProxy.getNearestNeighbors(currentHostIds, key, k);
            if (candidates.size() == k) {
                foundK = true;
            }
            hops++;
        } while (!foundK && (regionBitWidth + hops) <= depth);

        return candidates;
    }

    /**
     * Perform a radius search to check if there are any neighbours nearer to the query point than the
     * neighbours found on the query host server.
     *
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @param candidates                The nearest neighbours on the query point's host server.
     * @return                          The k nearest neighbour points.
     */
    List<long[]> radiusSearch(long[] key, int k, List<long[]> candidates, PHTreeIndexProxy<V> indexProxy) {
        return radiusStrategy.radiusSearch(key, k, candidates, indexProxy);
    }
}
