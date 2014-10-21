package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.disindex.codec.ByteRequestEncoder;
import ch.ethz.globis.disindex.codec.ByteResponseDecoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import ch.ethz.globis.distindex.ClusterService;
import ch.ethz.globis.distindex.client.DistributedIndexProxy;
import ch.ethz.globis.distindex.client.io.DefaultMessageService;
import ch.ethz.globis.distindex.mapping.NonDistributedMapping;
import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.distindex.mapping.bst.BSTMapping;
import ch.ethz.globis.distindex.mapping.bst.LongArrayKeyConverter;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;

public class DistributedPHTree<V> extends DistributedIndexProxy<long[], V> {

    public DistributedPHTree(String host, int port, Class<V> clazz) {
        FieldEncoderDecoder<long[]> keyEncoder = new MultiLongEncoderDecoder();
        FieldEncoderDecoder<V> valueEncoder = new SerializingEncoderDecoder<>(clazz);
        encoder = new ByteRequestEncoder<>(keyEncoder, valueEncoder);
        decoder = new ByteResponseDecoder<>(valueEncoder);

        service = new DefaultMessageService();
        clusterService = new ZKClusterService(host + ":" + port);
        clusterService.connect();
    }

    public void close() {
        clusterService.disconnect();
    }
}