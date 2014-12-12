package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.disindex.codec.io.RequestDispatcher;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.operation.request.GetKNNRequest;
import ch.ethz.globis.distindex.operation.request.Requests;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ZMappingKNNStrategy<V> implements KNNStrategy<V> {

    private static final Logger LOG = LoggerFactory.getLogger(ZMappingKNNStrategy.class);
    private KNNRadiusStrategy radiusStrategy = new RangeFilteredKNNRadiusStrategy();

    @Override
    public List<long[]> getNearestNeighbors(long[] key, int k, PHTreeIndexProxy<V> indexProxy) {
        LOG.debug("KNN request started for key={} and k={}", Arrays.toString(key), k);
        KeyMapping<long[]> keyMapping = indexProxy.getMapping();
        String keyHostId = keyMapping.get(key);
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

    private List<long[]> getNearestNeighbors(String hostId, long[] key, int k, PHTreeIndexProxy<V> indexProxy) {
        Requests<long[], byte[]> requests = new Requests<>(indexProxy.getClusterService());

        GetKNNRequest<long[]> request = requests.newGetKNN(key, k);
        RequestDispatcher<long[], V> requestDispatcher = indexProxy.getRequestDispatcher();
        ResultResponse<long[], V> response = requestDispatcher.send(hostId, request, ResultResponse.class);
        return indexProxy.extractKeys(response);
    }

    private List<long[]> radiusSearch(long[] key, int k, List<long[]> candidates, PHTreeIndexProxy<V> indexProxy) {
        return radiusStrategy.radiusSearch(key, k, candidates, indexProxy);
    }

    private List<long[]> iterativeExpansion(KeyMapping<long[]> keyMapping, long[] key, int k, PHTreeIndexProxy<V> indexProxy) {
        List<String> hostIds = keyMapping.get();
        return indexProxy.getNearestNeighbors(hostIds, key, k);
    }
}