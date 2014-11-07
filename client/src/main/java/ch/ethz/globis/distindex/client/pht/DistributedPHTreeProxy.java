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
import ch.ethz.globis.distindex.operation.GetKNNRequest;
import ch.ethz.globis.distindex.operation.Requests;
import ch.ethz.globis.distindex.operation.ResultResponse;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;
import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.pht.BitTools;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

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
        long[] farthestNeighbor = neighbours.get(k - 1);

        List<String> hostIds = keyMapping.getHostIds();

        GetKNNRequest<long[]> request = Requests.newGetKNN(key, k);
        List<ResultResponse<long[], V>> responses = requestDispatcher.send(hostIds, request);
        return MultidimUtil.nearestNeighbours(key, k, combineKeys(responses));
    }

    /**
     * Find all the hosts that need to be checked to find points inside the multi-dimensional
     * ball B(query, dist(query - neighbor)).
     *
     * @param query                                 The center of the multi-dimensional ball.
     * @param neighbor                              A point on the surface of the ball. dist(query, neighbor) determines
     *                                              the radius of the sphere.
     * @return                                      The hostId's whose assigned spatial regions intersect with the ball.
     */
    private List<String> additionalHostsToCheck(long[] query, long[] neighbor) {
        int dim = query.length;
        if (dim != neighbor.length) {
            throw new IllegalArgumentException("The points must have the same dimensionality");
        }
        String queryZ = getZRepresentation(query);
        String neighZ = getZRepresentation(neighbor);
        int prefix = StringUtils.indexOfDifference(queryZ, neighZ) / dim;
        String zRegionOfInterest = queryZ.substring(0, prefix);
        //now just need to find the neighbour regions of zRegionOfInterest in a Z-order curve of prefix iterations and dim
        //dimensions and see which hosts hold these regions

        return new ArrayList<>();
    }

    private String getZRepresentation(long[] point) {
        long[] mergedBits = BitTools.mergeLong(64, point);
        String bitString = "";
        for (long value : mergedBits) {
            bitString += longToString(value);
        }
        return bitString;
    }

    private String longToString(long l) {
        String bitString = Long.toBinaryString(l);
        int padding = 64 - bitString.length();
        String output = "";
        while (padding > 0) {
            padding--;
            output += "0";
        }
        return output + bitString;
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