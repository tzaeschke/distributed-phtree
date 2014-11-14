package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.distindex.middleware.net.RequestHandler;
import ch.ethz.globis.distindex.operation.*;
import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
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
                case OpCode.DELETE:
                    response = handleDeleteRequest(buffer);
                    break;
                case OpCode.GET_SIZE:
                    response = handleGetSizeRequest(buffer);
                    break;
                case OpCode.GET_DIM:
                    response = handleGetDimRequest(buffer);
                    break;
                case OpCode.GET_DEPTH:
                    response = handleGetDepthRequest(buffer);
                    break;
                case OpCode.CLOSE_ITERATOR:
                    response = handleCloseIterator(buffer);
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

    private ByteBuffer handleCloseIterator(ByteBuffer buffer) {
        MapRequest request = decoder.decodeMap(buffer);
        IntegerResponse response = requestHandler.handleCloseIterator(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleGetDimRequest(ByteBuffer buffer) {
        BaseRequest request = decoder.decodeBase(buffer);
        IntegerResponse response = requestHandler.handleGetDim(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleGetDepthRequest(ByteBuffer buffer) {
        BaseRequest request = decoder.decodeBase(buffer);
        IntegerResponse response = requestHandler.handleGetDepth(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleGetSizeRequest(ByteBuffer buffer) {
        BaseRequest request = decoder.decodeBase(buffer);
        IntegerResponse response = requestHandler.handleGetSize(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleDeleteRequest(ByteBuffer buf) {
        DeleteRequest<K> request = decoder.decodeDelete(buf);
        ResultResponse<K, V> response = requestHandler.handleDelete(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleGetBatchRequest(ByteBuffer buf) {
        GetIteratorBatchRequest<K> request = decoder.decodeGetBatch(buf);
        ResultResponse<K, V> response = requestHandler.handleGetIteratorBatch(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleCreateRequest(ByteBuffer buf) {
        CreateRequest request = decoder.decodeCreate(buf);
        ResultResponse<K, V> response = requestHandler.handleCreate(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handlePutRequest(ByteBuffer buf) {
        PutRequest<K, V> request = decoder.decodePut(buf);
        ResultResponse<K, V> response = requestHandler.handlePut(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleGetRequest(ByteBuffer buf) {
        GetRequest<K> request = decoder.decodeGet(buf);
        ResultResponse<K, V> response = requestHandler.handleGet(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleGetRangeRequest(ByteBuffer buf) {
        GetRangeRequest<K> request = decoder.decodeGetRange(buf);
        ResultResponse<K, V> response = requestHandler.handleGetRange(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleGetKNNRequest(ByteBuffer buf) {
        GetKNNRequest<K> request = decoder.decodeGetKNN(buf);
        ResultResponse<K, V> response = requestHandler.handleGetKNN(request);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    private ByteBuffer handleErroneousRequest(ByteBuffer buf) {
        ResultResponse<K, V> response = new ResultResponse<>((byte) 0, 0, OpStatus.FAILURE);
        byte[] responseBytes = encoder.encode(response);
        return ByteBuffer.wrap(responseBytes);
    }

    public RequestHandler<K, V> getRequestHandler() {
        return requestHandler;
    }

    private byte getMessageCode(ByteBuffer buffer) {
        return buffer.get(0);
    }
}