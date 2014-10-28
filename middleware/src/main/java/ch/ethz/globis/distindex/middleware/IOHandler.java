package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.middleware.net.RequestHandler;
import ch.ethz.globis.distindex.operation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class IOHandler<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(IOHandler.class);

    private RequestHandler<K, V> requestHandler;
    private RequestDecoder<K, V> decoder;
    private ResponseEncoder<K, V> encoder;

    public IOHandler(RequestHandler<K, V> requestHandler, RequestDecoder<K, V> decoder, ResponseEncoder<K, V> encoder) {
        this.requestHandler = requestHandler;
        this.decoder = decoder;
        this.encoder = encoder;
    }

    public ByteBuffer handle(ByteBuffer buffer) {
        byte messageCode = getMessageCode(buffer);

        ByteBuffer response;
        try {
            switch (messageCode) {
                case OpCode.GET:
                    response = handleGetRequest(buffer);
                    break;
                case OpCode.GET_KNN:
                    response = handleGetKNNRequest(buffer);
                    break;
                case OpCode.GET_RANGE:
                    response = handleGetRangeRequest(buffer);
                    break;
                case OpCode.GET_BATCH:
                    response = handleGetBatchRequest(buffer);
                    break;
                case OpCode.PUT:
                    response = handlePutRequest(buffer);
                    break;
                case OpCode.CREATE_INDEX:
                    response = handleCreateRequest(buffer);
                    break;
                default:
                    response = handleErroneousRequest(buffer);
            }
        } catch (Exception e) {
            LOG.error("Error processing request", e);
            response = handleErroneousRequest(buffer);
        }
        return response;
    }

    private ByteBuffer handleGetBatchRequest(ByteBuffer buf) {
        GetIteratorBatch request = decoder.decodeGetBatch(buf);
        Response<K, V> response = requestHandler.handleGetIteratorBatch(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleCreateRequest(ByteBuffer buf) {
        CreateRequest request = decoder.decodeCreate(buf);
        Response<K, V> response = requestHandler.handleCreate(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handlePutRequest(ByteBuffer buf) {
        PutRequest<K, V> request = decoder.decodePut(buf);
        Response<K, V> response = requestHandler.handlePut(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleGetRequest(ByteBuffer buf) {
        GetRequest<K> request = decoder.decodeGet(buf);
        Response<K, V> response = requestHandler.handleGet(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleGetRangeRequest(ByteBuffer buf) {
        GetRangeRequest<K> request = decoder.decodeGetRange(buf);
        Response<K, V> response = requestHandler.handleGetRange(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleGetKNNRequest(ByteBuffer buf) {
        GetKNNRequest<K> request = decoder.decodeGetKNN(buf);
        Response<K, V> response = requestHandler.handleGetKNN(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleErroneousRequest(ByteBuffer buf) {
        Response<K, V> response = new Response<>((byte) 0, 0, OpStatus.FAILURE);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private byte getMessageCode(ByteBuffer buffer) {
        return buffer.get(0);
    }
}