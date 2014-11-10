package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.disindex.codec.ByteRequestEncoder;
import ch.ethz.globis.disindex.codec.ByteResponseDecoder;
import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import ch.ethz.globis.distindex.api.PointIndex;
import ch.ethz.globis.distindex.client.DistributedIndexProxy;
import ch.ethz.globis.distindex.client.io.ClientRequestDispatcher;
import ch.ethz.globis.distindex.client.io.RequestDispatcher;
import ch.ethz.globis.distindex.client.io.TCPClient;
import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.distindex.client.io.Transport;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.mapping.ZCurveHelper;
import ch.ethz.globis.distindex.operation.GetKNNRequest;
import ch.ethz.globis.distindex.operation.Requests;
import ch.ethz.globis.distindex.operation.ResultResponse;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;
import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.pht.BitTools;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *  Represents a proxy to a distributed multi-dimensional index. The API implemented is independent of any
 *  multi-dimensional index API.
 *
 * @param <V>                               The value class for this index.
 */
public class DistributedPHTreeProxy<V> extends DistributedIndexProxy<long[], V> implements PointIndex<V>{

    public DistributedPHTreeProxy(String host, int port) {
        requestDispatcher = setupDispatcher();
        clusterService = setupClusterService(host, port);
        clusterService.connect();
    }

    private RequestDispatcher<long[], V> setupDispatcher() {
        FieldEncoderDecoder<long[]> keyEncoder = new MultiLongEncoderDecoder();
        FieldEncoderDecoder<V> valueEncoder = new SerializingEncoderDecoder<>();
        RequestEncoder<long[], V> encoder = new ByteRequestEncoder<>(keyEncoder, valueEncoder);
        ResponseDecoder<long[], V> decoder = new ByteResponseDecoder<>(keyEncoder, valueEncoder);
        Transport transport = new TCPClient();

        return new ClientRequestDispatcher<>(transport, encoder, decoder);
    }

    @Override
    public List<long[]> getNearestNeighbors(long[] key, int k) {

        KeyMapping<long[]> keyMapping = clusterService.getMapping();

        //return getNearestNeighbors(keyMapping.getHostIds(), key, k);

        String keyHostId = keyMapping.getHostId(key);
        List<long[]> neighbours = getNearestNeighbors(keyHostId, key, k);
        if (neighbours.size() < k) {
            //ToDo need to gradually expand the number of nodes to query
            return getNearestNeighbors(keyMapping.getHostIds(), key, k);
        }
        if (k <= 0) {
            return neighbours;
        }
        long[] farthestNeighbor = neighbours.get(k - 1);
        List<long[]> neighbors = ZCurveHelper.getNeighbours(key, farthestNeighbor);

        Set<String> neighbourHosts = new HashSet<>();
        for (long[] neighbour : neighbors) {
            //ToDo the neighbour rectangle size is dim * size, which could correspond to the zones of more hosts, need to make sure this case is resolved
            neighbourHosts.add(keyMapping.getHostId(neighbour));
        }

        GetKNNRequest<long[]> request = Requests.newGetKNN(key, k);
        List<ResultResponse<long[], V>> responses = requestDispatcher.send(neighbourHosts, request);
        return MultidimUtil.nearestNeighbours(key, k, combineKeys(responses));
    }

    private List<long[]> getNearestNeighbors(String hostId, long[] key, int k) {
        GetKNNRequest<long[]> request = Requests.newGetKNN(key, k);
        ResultResponse<long[], V> response = requestDispatcher.send(hostId, request);
        return extractKeys(response);
    }

    private List<long[]> getNearestNeighbors(List<String> hostIds, long[] key, int k) {
        GetKNNRequest<long[]> request = Requests.newGetKNN(key, k);
        List<ResultResponse<long[], V>> responses = requestDispatcher.send(hostIds, request);
        return MultidimUtil.nearestNeighbours(key, k, combineKeys(responses));
    }

    private ClusterService<long[]> setupClusterService(String host, int port) {
        return new ZKClusterService(host + ":" + port);
    }
}