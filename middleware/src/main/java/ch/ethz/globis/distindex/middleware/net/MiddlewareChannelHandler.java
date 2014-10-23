package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.disindex.codec.OpCode;
import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.disindex.codec.util.Pair;
import ch.ethz.globis.distindex.middleware.pht.PHTreeIndexAdaptor;
import ch.ethz.globis.distindex.shared.Index;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.List;

public abstract class MiddlewareChannelHandler<K, V> extends ChannelInboundHandlerAdapter {

    /** The backing index*/
    protected Index<K, V> index;

    /** Encoder for responses */
    protected ResponseEncoder<K, V> encoder;

    /** Decoder for requests. */
    protected RequestDecoder<K, V> decoder;

    protected MiddlewareChannelHandler(Index<K, V> index,
                                       ResponseEncoder<K, V> encoder,
                                       RequestDecoder<K, V> decoder) {
        this.index = index;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        byte messageCode = getMessageCode(buf);

        ByteBuf response;
        switch (messageCode) {
            case OpCode.GET:
                response = handleGetRequest(buf);
                break;
            case OpCode.GET_KNN:
                response = handleGetKNNRequest(buf);
                break;
            case OpCode.GET_RANGE:
                response = handleGetRangeRequest(buf);
                break;
            case OpCode.PUT:
                response = handlePutRequest(buf);
                break;
            case OpCode.CREATE_INDEX:
                response = handleCreateRequest(buf);
                break;
            default:
                response = handleErroneousRequest(buf);
        }
        ctx.writeAndFlush(response);
        buf.release();
    }

    private ByteBuf handleCreateRequest(ByteBuf buf) {
        Pair<Integer, Integer> pair = decoder.decodeCreate(buf.nioBuffer());
        index = (Index<K, V>) new PHTreeIndexAdaptor<V>(pair.getFirst(), pair.getSecond());
        byte[] response = encoder.encoderCreate();
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
        return buf.readByte();
    }
}
