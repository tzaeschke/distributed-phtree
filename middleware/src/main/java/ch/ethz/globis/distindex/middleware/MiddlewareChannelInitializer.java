package ch.ethz.globis.distindex.middleware;

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
 * @param <V>
 */
public class MiddlewareChannelInitializer<V> extends ChannelInitializer<SocketChannel> {

    private Properties properties;

    public MiddlewareChannelInitializer(Properties properties) {
        this.properties = properties;
    }

    private Index<long[], V> index;

    private Class<V> clazz;

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        if (index == null) {
            int dim = Integer.parseInt(properties.getProperty(IndexProperties.INDEX_DIM));
            int depth = Integer.parseInt(properties.getProperty(IndexProperties.INDEX_DEPTH));
            String valueClass = properties.getProperty(IndexProperties.INDEX_VALUE_CLASS);

            clazz = (Class<V>) Class.forName(valueClass);
            index = new PHTreeIndexAdaptor<>(dim, depth);
        }

        RequestDecoder<long[], V> requestDecoder = new ByteRequestDecoder<>(new MultiLongEncoderDecoder(), new SerializingEncoderDecoder<>(clazz));
        ResponseEncoder<long[], V> responseEncoder = new ByteResponseEncoder<>(new MultiLongEncoderDecoder(), new SerializingEncoderDecoder<>(clazz));

        pipeline.addLast(new MultidimMiddlewareChannelHandler<>(index, responseEncoder, requestDecoder));
    }

}
