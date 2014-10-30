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
import ch.ethz.globis.distindex.operation.Response;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;
import ch.ethz.globis.distindex.util.MultidimUtil;

import java.util.List;

public class DistributedPHTree<V> extends DistributedIndexProxy<long[], V> implements PointIndex<V>{

    public DistributedPHTree(String host, int port, Class<V> clazz) {
        requestDispatcher = setupDispatcher(clazz);
        clusterService = setupClusterService(host, port);
        clusterService.connect();
    }

    @Override
    public List<long[]> getNearestNeighbors(long[] key, int k) {
        KeyMapping<long[]> keyMapping = clusterService.getMapping();
        List<String> hostIds = keyMapping.getHostIds();

        GetKNNRequest<long[]> request = Requests.newGetKNN(key, k);
        List<Response<long[], V>> responses = requestDispatcher.send(hostIds, request);
        return MultidimUtil.nearestNeighbours(key, k, combineKeys(responses));
    }

    private ClusterService<long[]> setupClusterService(String host, int port) {
        return new ZKClusterService(host + ":" + port);
    }

    private RequestDispatcher<long[], V> setupDispatcher(Class<V> clazz) {
        FieldEncoderDecoder<long[]> keyEncoder = new MultiLongEncoderDecoder();
        FieldEncoderDecoder<V> valueEncoder = new SerializingEncoderDecoder<>(clazz);
        RequestEncoder<long[], V> encoder = new ByteRequestEncoder<>(keyEncoder, valueEncoder);
        ResponseDecoder<long[], V> decoder = new ByteResponseDecoder<>(keyEncoder, valueEncoder);
        Transport transport = new TCPClient();

        return new ClientRequestDispatcher<>(transport, encoder, decoder);
    }
}