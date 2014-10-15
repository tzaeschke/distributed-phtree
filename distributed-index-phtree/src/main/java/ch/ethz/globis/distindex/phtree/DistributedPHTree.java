package ch.ethz.globis.distindex.phtree;

import ch.ethz.globis.disindex.codec.ByteRequestEncoder;
import ch.ethz.globis.disindex.codec.ByteResponseDecoder;
import ch.ethz.globis.distindex.client.DistributedIndex;
import ch.ethz.globis.distindex.client.io.DefaultMessageService;
import ch.ethz.globis.distindex.client.mapping.NonDistributedMapping;
import ch.ethz.globis.distindex.codec.FieldEncoder;

public class DistributedPHTree<V> extends DistributedIndex<long[], V> {

    public DistributedPHTree(String host, int port) {
        FieldEncoder<long[]> keyEncoder = null;
        FieldEncoder<V> valueEncoder = null;
        encoder = new ByteRequestEncoder<>(keyEncoder, valueEncoder);
        decoder = new ByteResponseDecoder<>();
        service = new DefaultMessageService(port);
        keyMapping = new NonDistributedMapping(host);
    }
}
