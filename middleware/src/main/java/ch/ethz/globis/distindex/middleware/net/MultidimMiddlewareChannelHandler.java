package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.disindex.codec.ByteResponseEncoder;
import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.distindex.api.Index;

public class MultidimMiddlewareChannelHandler extends MiddlewareChannelHandler<long[], byte[]> {

    protected MultidimMiddlewareChannelHandler(Index<long[], byte[]> index, ByteResponseEncoder<long[]> encoder,
                                               RequestDecoder<long[], byte[]> decoder) {
        super(index, encoder, decoder);
    }
}
