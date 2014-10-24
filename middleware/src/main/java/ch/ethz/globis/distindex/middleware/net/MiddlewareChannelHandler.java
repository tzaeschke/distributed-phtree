package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.operation.*;
import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.distindex.middleware.pht.PHTreeIndexAdaptor;
import ch.ethz.globis.distindex.api.Index;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

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
        CreateRequest request = decoder.decodeCreate(buf.nioBuffer());
        index = (Index<K, V>) new PHTreeIndexAdaptor<V>(request.getDim(), request.getDepth());
        Response<K, V> response = new Response<>(request.getOpCode(), request.getId(), OpStatus.SUCCESS, new IndexEntryList<K,V>());

        byte[] responseBytes = encoder.encode(response);
        return Unpooled.wrappedBuffer(responseBytes);
    }

    private ByteBuf handlePutRequest(ByteBuf buf) {
        PutRequest<K, V> request = decoder.decodePut(buf.nioBuffer());
        K key = request.getKey();
        V value = request.getValue();
        index.put(key, value);
        return createResult(request, new IndexEntryList<>(key, value));
    }

    private ByteBuf handleGetRequest(ByteBuf buf) {
        GetRequest<K> request = decoder.decodeGet(buf.nioBuffer());
        K key = request.getKey();
        V value = index.get(key);
        if (value == null) {
            return createResult(request, new IndexEntryList<K, V>());
        } else {
            return createResult(request, new IndexEntryList<>(key, value));
        }

    }

    private ByteBuf handleGetRangeRequest(ByteBuf buf) {
        GetRangeRequest<K> request = decoder.decodeGetRange(buf.nioBuffer());
        IndexEntryList<K, V> values = index.getRange(request.getStart(), request.getEnd());

        return createResult(request, values);
    }

    private ByteBuf handleGetKNNRequest(ByteBuf buf) {
        GetKNNRequest<K> request = decoder.decodeGetKNN(buf.nioBuffer());
        IndexEntryList<K, V> values = index.getNearestNeighbors(request.getKey(), request.getK());

        return createResult(request, values);
    }

    private ByteBuf handleErroneousRequest(ByteBuf buf) {
        //ToDo need better handling
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

    private ByteBuf createResult(Request request, IndexEntryList<K, V> values) {
        Response<K, V> response = new Response<>(request.getOpCode(), request.getId(), OpStatus.SUCCESS, values);
        byte[] responseBytes = encoder.encode(response);
        return Unpooled.wrappedBuffer(responseBytes);
    }

    private byte getMessageCode(ByteBuf buf) {
        return buf.getByte(0);
    }
}
