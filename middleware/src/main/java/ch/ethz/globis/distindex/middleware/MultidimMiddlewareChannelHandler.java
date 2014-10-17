package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.distindex.shared.Index;

public class MultidimMiddlewareChannelHandler<V> extends MiddlewareChannelHandler<long[], V> {

    protected MultidimMiddlewareChannelHandler(Index<long[], V> index, ResponseEncoder<long[], V> encoder, RequestDecoder<long[], V> decoder) {
        super(index, encoder, decoder);
    }
}
