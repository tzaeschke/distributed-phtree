package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.distindex.api.Index;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.middleware.pht.PHTreeIndexAdaptor;
import ch.ethz.globis.distindex.operation.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MiddlewareChannelHandler<K, V> extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(MiddlewareChannelHandler.class);

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
        try {
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
                case OpCode.GET_BATCH:
                    response = handleGetBatchRequest(buf);
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
        } catch (Exception e) {
            LOG.error("Error processing request", e);
            response = handleErroneousRequest(buf);
        }
        ctx.writeAndFlush(response);
        buf.release();
    }

    private ByteBuf handleGetBatchRequest(ByteBuf buf) {
        GetBatchRequest<K> request = decoder.decodeGetBatch(buf.nioBuffer());
        K startKey = request.getKey();
        int size = request.getSize();
        IndexEntryList<K, V> entries = index.getBatch(startKey, size);

        return createResult(request, entries);
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
        Response<K, V> response = new Response<>((byte) 0, 0, OpStatus.FAILURE, new IndexEntryList<K, V>());
        byte[] responseBytes = encoder.encode(response);
        return Unpooled.wrappedBuffer(responseBytes);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("Exception occurred on the channel: " + ctx, cause);
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