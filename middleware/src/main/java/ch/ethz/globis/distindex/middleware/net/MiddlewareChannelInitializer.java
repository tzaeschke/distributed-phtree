package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.disindex.codec.ByteRequestDecoder;
import ch.ethz.globis.disindex.codec.ByteResponseEncoder;
import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import ch.ethz.globis.distindex.middleware.config.IndexProperties;
import ch.ethz.globis.distindex.middleware.pht.PHTreeIndexAdaptor;
import ch.ethz.globis.distindex.shared.Index;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

import java.util.Properties;

/**
 * Initialize the handler for a new accepted connection.
 *
 */
public class MiddlewareChannelInitializer extends ChannelInitializer<SocketChannel> {

    private Properties properties;

    public MiddlewareChannelInitializer(Properties properties) {
        this.properties = properties;
    }

    private Index<long[], byte[]> index;

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

//        if (index == null) {
//            int dim = Integer.parseInt(properties.getProperty(IndexProperties.INDEX_DIM));
//            int depth = Integer.parseInt(properties.getProperty(IndexProperties.INDEX_DEPTH));
//            index = new PHTreeIndexAdaptor<>(dim, depth);
//        }

        ByteRequestDecoder<long[]> requestDecoder = new ByteRequestDecoder<>(new MultiLongEncoderDecoder());
        ByteResponseEncoder<long[]> responseEncoder = new ByteResponseEncoder<>(new MultiLongEncoderDecoder());

        pipeline.addLast(new MultidimMiddlewareChannelHandler(index, responseEncoder, requestDecoder));
    }

}
