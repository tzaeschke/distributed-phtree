package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.disindex.codec.ByteRequestEncoder;
import ch.ethz.globis.disindex.codec.ByteResponseDecoder;
import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.api.PointIndex;
import ch.ethz.globis.distindex.client.IndexProxy;
import ch.ethz.globis.disindex.codec.io.ClientRequestDispatcher;
import ch.ethz.globis.disindex.codec.io.RequestDispatcher;
import ch.ethz.globis.disindex.codec.io.TCPClient;
import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.disindex.codec.io.Transport;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.operation.request.GetKNNRequest;
import ch.ethz.globis.distindex.operation.request.GetRangeRequest;
import ch.ethz.globis.distindex.operation.request.Requests;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.BSTMapClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;
import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.PhTreeQStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *  Represents a proxy to a distributed multi-dimensional index. The API implemented is independent of any
 *  multi-dimensional index API.
 *
 * @param <V>                               The value class for this index.
 */
public class PHTreeIndexProxy<V> extends IndexProxy<long[], V> implements PointIndex<V>{

    private static final Logger LOG = LoggerFactory.getLogger(PHTreeIndexProxy.class);

    private int depth = -1;
    private int dim = -1;

    private KNNRadiusStrategy knnRadiusStrategy = new RangeKNNRadiusStrategy();

    private KNNStrategy<V> knnStrategy = new ZMappingKNNStrategy<>();

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

    public boolean create(final int dim, final int depth) {
        this.dim = dim;
        this.depth = depth;
        Map<String, String> options = new HashMap<String, String>() {{
            put("dim", String.valueOf(dim));
            put("depth", String.valueOf(depth));
        }};
        return super.create(options);
    }

    /**
     * Perform a range query and then filter using a distance.
     *
     * @param start
     * @param end
     * @param distance
     * @return
     */
    public IndexEntryList<long[], V> getRange(long[] start, long[] end, double distance) {
        LOG.debug("Get Range request started on interval {} and distance {}",
                Arrays.toString(start) + "-" + Arrays.toString(end), distance);

        KeyMapping<long[]> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.get(start, end);

        GetRangeRequest<long[]> request = Requests.newGetRange(start, end, distance);
        List<ResultResponse> responses = requestDispatcher.send(hostIds, request, ResultResponse.class);
        LOG.debug("Get Range request ended on interval {} and distance {}",
                Arrays.toString(start) + "-" + Arrays.toString(end), distance);
        return combine(responses);
    }

    /**
     * Find the k nearest neighbours of a query point.
     * @param key                       The key to be used as query.
     * @param k                         The number of neighbours to be returned.
     * @return
     */
    @Override
    public List<long[]> getNearestNeighbors(long[] key, int k) {
        return knnStrategy.getNearestNeighbors(key, k, this);
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
        logKNNRequest(hostId, key, k);

        GetKNNRequest<long[]> request = Requests.newGetKNN(key, k);
        RequestDispatcher<long[], V> requestDispatcher = getRequestDispatcher();
        ResultResponse<long[], V> response = requestDispatcher.send(hostId, request, ResultResponse.class);
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
        logKNNRequest(hostIds, key, k);

        GetKNNRequest<long[]> request = Requests.newGetKNN(key, k);
        List<ResultResponse> responses = getRequestDispatcher().send(hostIds, request, ResultResponse.class);
        return MultidimUtil.nearestNeighbours(key, k, combineKeys(responses));
    }

    public PhTree.Stats getStats() {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public PhTree.Stats getStatsIdealNoNode() {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public PhTreeQStats getQuality() {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public int getNodeCount() {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public String toStringPlain() {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    public String toStringTree() {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    private ClusterService<long[]> setupClusterService(String host, int port) {
        return new ZKClusterService(host + ":" + port);
    }

    public void setKnnRadiusStrategy(KNNRadiusStrategy knnRadiusStrategy) {
        this.knnRadiusStrategy = knnRadiusStrategy;
    }

    public KeyMapping<long[]> getMapping() {
        return clusterService.getMapping();
    }

    RequestDispatcher<long[], V> getRequestDispatcher() {
        return requestDispatcher;
    }

    ClusterService<long[]> getClusterService() {
        return clusterService;
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