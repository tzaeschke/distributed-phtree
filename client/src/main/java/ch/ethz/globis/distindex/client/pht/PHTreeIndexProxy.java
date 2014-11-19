package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.disindex.codec.ByteRequestEncoder;
import ch.ethz.globis.disindex.codec.ByteResponseDecoder;
import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import ch.ethz.globis.distindex.api.PointIndex;
import ch.ethz.globis.distindex.client.IndexProxy;
import ch.ethz.globis.distindex.client.io.ClientRequestDispatcher;
import ch.ethz.globis.distindex.client.io.RequestDispatcher;
import ch.ethz.globis.distindex.client.io.TCPClient;
import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.distindex.client.io.Transport;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.mapping.ZCurveHelper;
import ch.ethz.globis.distindex.operation.request.GetKNNRequest;
import ch.ethz.globis.distindex.operation.request.Requests;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;
import ch.ethz.globis.distindex.util.MultidimUtil;

import java.util.*;

/**
 *  Represents a proxy to a distributed multi-dimensional index. The API implemented is independent of any
 *  multi-dimensional index API.
 *
 * @param <V>                               The value class for this index.
 */
public class PHTreeIndexProxy<V> extends IndexProxy<long[], V> implements PointIndex<V>{

    private int depth = -1;
    private int dim = -1;

    private KNNStrategy knnStrategy = new RangeKNNStrategy();

    public PHTreeIndexProxy(ClusterService<long[]> clusterService) {
        this.clusterService = clusterService;
        this.requestDispatcher = setupDispatcher();
        this.clusterService.connect();
    }

    public PHTreeIndexProxy(String host, int port) {
        requestDispatcher = setupDispatcher();
        clusterService = setupClusterService(host, port);
        clusterService.connect();
    }

    private RequestDispatcher<long[], V> setupDispatcher() {
        FieldEncoderDecoder<long[]> keyEncoder = new MultiLongEncoderDecoder();
        FieldEncoderDecoder<V> valueEncoder = new SerializingEncoderDecoder<>();
        RequestEncoder encoder = new ByteRequestEncoder<>(keyEncoder, valueEncoder);
        ResponseDecoder<long[], V> decoder = new ByteResponseDecoder<>(keyEncoder, valueEncoder);
        Transport transport = new TCPClient();

        return new ClientRequestDispatcher<>(transport, encoder, decoder);
    }

    @Override
    public boolean create(int dim, int depth) {
        this.dim = dim;
        this.depth = depth;
        return super.create(dim, depth);
    }

    /**
     * Find the k nearest neighbours of a query point.
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @return
     */
    @Override
    public List<long[]> getNearestNeighbors(long[] key, int k) {

        KeyMapping<long[]> keyMapping = clusterService.getMapping();

        String keyHostId = keyMapping.getHostId(key);
        List<long[]> candidates = getNearestNeighbors(keyHostId, key, k);
        if (candidates.size() < k) {
            return iterativeExpansion(keyMapping, key, k);
        }
        return radiusSearch(key, k, candidates);
    }

    /**
     * Find the k nearest neighbours of a query point from the host with the id hostId.
     *
     * @param hostId                    The id of the host on which the query is run.
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @return                          The k nearest neighbours on the host.
     */
    List<long[]> getNearestNeighbors(String hostId, long[] key, int k) {
        GetKNNRequest<long[]> request = Requests.newGetKNN(key, k);
        ResultResponse<long[], V> response = requestDispatcher.send(hostId, request);
        return extractKeys(response);
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
    List<long[]> getNearestNeighbors(Collection<String> hostIds, long[] key, int k) {
        GetKNNRequest<long[]> request = Requests.newGetKNN(key, k);
        List<ResultResponse<long[], V>> responses = requestDispatcher.send(hostIds, request);
        return MultidimUtil.nearestNeighbours(key, k, combineKeys(responses));
    }

    /**
     * Perform an iterative expansion search for the nearest neighbour. This should be called if
     * the host containing the query point does not contain K nearest neighbours.
     *
     * @param key                       The query point.
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     */
    List<long[]> iterativeExpansion(KeyMapping<long[]> keyMapping, long[] key, int k) {
        List<String> allHostIds = keyMapping.getHostIds();
        String hostId = keyMapping.getHostId(key);

        List<long[]> candidates;
        int size = depth - keyMapping.getDepth(hostId) / dim;
        Set<String> currentHostIds;
        boolean foundK = false;
        int hops = 1;
        do {
            List<long[]> projections = ZCurveHelper.getProjectionsWithinHops(key, hops, size);
            currentHostIds = keyMapping.getHostsContaining(projections);
            candidates = getNearestNeighbors(currentHostIds, key, k);
            if (candidates.size() == k) {
                foundK = true;
            }
            hops++;
        } while (!foundK && (currentHostIds.size() < allHostIds.size()));

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
    List<long[]> radiusSearch(long[] key, int k, List<long[]> candidates) {
        return knnStrategy.radiusSearch(key, k, candidates, this);
    }

    long computeDistance(long[] a, long[] b) {
        long dist = 0;
        for (int i = 0; i < a.length; i++) {
            dist += (a[i] - b[i]) * (a[i] - b[i]);
        }
        return (long) Math.sqrt(dist) + 1;
    }

    long[] transpose(long[] a, long offset) {
        long[] result = Arrays.copyOf(a, a.length);
        for (int i = 0; i < a.length; i++) {
            result[i] += offset;
        }
        return result;
    }

    private ClusterService<long[]> setupClusterService(String host, int port) {
        return new ZKClusterService(host + ":" + port);
    }

    public void setKnnStrategy(KNNStrategy knnStrategy) {
        this.knnStrategy = knnStrategy;
    }

    public KeyMapping<long[]> getMapping() {
        return clusterService.getMapping();
    }
}