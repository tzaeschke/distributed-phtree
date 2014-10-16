package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.disindex.codec.RequestCode;
import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.disindex.codec.util.Pair;
import ch.ethz.globis.distindex.shared.Index;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.List;

public class DefaultIndexServerHandler<K, V> extends ChannelInboundHandlerAdapter {

    /** The backing index*/
    private Index<K, V> index;

    /** Encoder for responses */
    private ResponseEncoder<K, V> encoder;

    /** Decoder for requests. */
    private RequestDecoder<K, V> decoder;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //ToDo send the data structure containing intervals
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        byte messageCode = getMessageCode(buf);

        ByteBuf response;
        switch (messageCode) {
            case RequestCode.GET:
                response = handleGetRequest(buf);
                break;
            case RequestCode.GET_KNN:
                response = handleGetKNNRequest(buf);
                break;
            case RequestCode.GET_RANGE:
                response = handleGetRangeRequest(buf);
                break;
            case RequestCode.PUT:
                response = handlePutRequest(buf);
                break;
            default:
                response = handleErroneousRequest(buf);
        }
        ctx.writeAndFlush(response);
    }

    private ByteBuf handleGetRequest(ByteBuf buf) {
        K key = decoder.decodeGet(buf.nioBuffer());
        V value = index.get(key);

        byte[] response = encoder.encodeGet(value);
        return Unpooled.wrappedBuffer(response);
    }

    private ByteBuf handleGetRangeRequest(ByteBuf buf) {
        Pair<K, K> range = decoder.decodeGetRange(buf.nioBuffer());
        List<V> values = index.getRange(range.getFirst(), range.getSecond());

        byte[] response = encoder.encodeGetRange(values);
        return Unpooled.wrappedBuffer(response);
    }

    private ByteBuf handleGetKNNRequest(ByteBuf buf) {
        Pair<K, Integer> range = decoder.decodeGetKNN(buf.nioBuffer());
        List<V> values = index.getNearestNeighbors(range.getFirst(), range.getSecond());

        byte[] response = encoder.encodeGetRange(values);
        return Unpooled.wrappedBuffer(response);
    }

    private ByteBuf handlePutRequest(ByteBuf buf) {
        Pair<K, V> entry = decoder.decodePut(buf.nioBuffer());
        K key = entry.getFirst();
        V value = entry.getSecond();
        index.put(key, value);

        byte[] response = encoder.encodePut(key, value);
        return Unpooled.wrappedBuffer(response);
    }

    private ByteBuf handleErroneousRequest(ByteBuf buf) {
        return emptyBuffer();
    }

    private ByteBuf emptyBuffer() {
        ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;
        return alloc.buffer();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.err.format("Exception!");
        cause.printStackTrace();
    }

    private byte getMessageCode(ByteBuf buf) {
        return buf.getByte(0);
    }
}
