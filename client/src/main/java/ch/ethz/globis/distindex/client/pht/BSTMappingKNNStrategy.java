package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.disindex.codec.io.RequestDispatcher;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.mapping.ZCurveHelper;
import ch.ethz.globis.distindex.mapping.bst.MultidimMapping;
import ch.ethz.globis.distindex.operation.request.GetKNNRequest;
import ch.ethz.globis.distindex.operation.request.Requests;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.distindex.util.MultidimUtil;
import org.slf4j.Logger;

import java.util.*;

public class BSTMappingKNNStrategy<V> implements KNNStrategy<V> {

    /** The logger used by this class. */
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(BSTMappingKNNStrategy.class);

    private KNNRadiusStrategy radiusStrategy = new RangeFilteredKNNRadiusStrategy();

    @Override
    public List<long[]> getNearestNeighbors(long[] key, int k, PHTreeIndexProxy<V> indexProxy) {
        LOG.debug("KNN request started for key={} and k={}", Arrays.toString(key), k);
        MultidimMapping keyMapping = (MultidimMapping) indexProxy.getMapping();
        String keyHostId = keyMapping.getHostId(key);
        List<long[]> candidates = getNearestNeighbors(keyHostId, key, k, indexProxy);
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
     * Find the k nearest neighbours of a query point from the host with the id hostId.
     *
     * @param hostId                    The id of the host on which the query is run.
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @return                          The k nearest neighbours on the host.
     */
    List<long[]> getNearestNeighbors(String hostId, long[] key, int k, PHTreeIndexProxy<V> indexProxy) {
        logKNNRequest(hostId, key, k);

        GetKNNRequest<long[]> request = Requests.newGetKNN(key, k);
        RequestDispatcher<long[], V> requestDispatcher = indexProxy.getRequestDispatcher();
        ResultResponse<long[], V> response = requestDispatcher.send(hostId, request, ResultResponse.class);
        return indexProxy.extractKeys(response);
    }

    /**
     *  Find the k nearest neighbours of a query point from the hosts with the ids contained
     *  int the hostIds list.
     *
     * @param hostIds
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @return                          The k nearest neighbours on the hosts.
     */
    static <V> List<long[]> getNearestNeighbors(Collection<String> hostIds, long[] key, int k, PHTreeIndexProxy<V> indexProxy) {
        logKNNRequest(hostIds, key, k);

        GetKNNRequest<long[]> request = Requests.newGetKNN(key, k);
        List<ResultResponse> responses = indexProxy.getRequestDispatcher().send(hostIds, request, ResultResponse.class);
        return MultidimUtil.nearestNeighbours(key, k, indexProxy.combineKeys(responses));
    }

    /**
     * Perform an iterative expansion search for the nearest neighbour. This should be called if
     * the host containing the query point does not contain K nearest neighbours.
     *
     * @param key                       The query point.
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     */
    <V> List<long[]> iterativeExpansion(KeyMapping<long[]> keyMapping, long[] key, int k, PHTreeIndexProxy<V> indexProxy) {
        int dim = indexProxy.getDim();
        int depth = indexProxy.getDepth();

        String hostId = keyMapping.getHostId(key);

        List<long[]> candidates;
        int regionBitWidth = depth - keyMapping.getDepth(hostId) / dim;
        Set<String> currentHostIds;
        boolean foundK = false;
        int hops = 1;
        do {
            if (hops + regionBitWidth > depth) {
                currentHostIds = new HashSet<>(keyMapping.getHostIds());
            } else {
                List<long[]> projections = ZCurveHelper.getProjectionsWithinHops(key, hops, regionBitWidth);
                currentHostIds = keyMapping.getHostsContaining(projections);
            }
            candidates = getNearestNeighbors(currentHostIds, key, k, indexProxy);
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
        return radiusStrategy.radiusSearch(key, k, candidates, (MultidimMapping) indexProxy.getMapping(), this, indexProxy);
    }

    static void logKNNRequest(String hostId, long[] key, int k) {
        LOG.debug("Sending kNN request with key = {} and k = {} to host" + hostId, Arrays.toString(key), k);
    }

    static void logKNNRequest(Collection<String> hostIds, long[] key, int k) {
        for (String hostId : hostIds) {
            logKNNRequest(hostId, key, k);
        }
    }
}
