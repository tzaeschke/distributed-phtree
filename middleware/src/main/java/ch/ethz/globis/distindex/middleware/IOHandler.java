package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.distindex.middleware.net.BalancingRequestHandler;
import ch.ethz.globis.distindex.middleware.net.RequestHandler;
import ch.ethz.globis.distindex.operation.*;
import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class IOHandler<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(IOHandler.class);

    private RequestHandler<K, V> requestHandler;
    private BalancingRequestHandler<K, V> balancingRequestHandler;
    private RequestDecoder<K, V> decoder;
    private ResponseEncoder encoder;

    public IOHandler(RequestHandler<K, V> requestHandler,
                     BalancingRequestHandler<K, V> balancingRequestHandler,
                     RequestDecoder<K, V> decoder,
                     ResponseEncoder encoder) {
        this.requestHandler = requestHandler;
        this.balancingRequestHandler = balancingRequestHandler;
        this.decoder = decoder;
        this.encoder = encoder;
    }

    public void cleanup(String clientHost) {
        requestHandler.cleanup(clientHost);
    }

    public ByteBuffer handle(String clientHost, ByteBuffer buffer) {
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
                    response = handleGetBatchRequest(clientHost, buffer);
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
                    response = handleCloseIterator(clientHost, buffer);
                    break;
                case OpCode.CONTAINS:
                    response = handleContains(buffer);
                    break;
                case OpCode.BALANCE_INIT:
                    response = handleBalanceInit(buffer);
                    break;
                case OpCode.BALANCE_PUT:
                    response = handleBalancePut(buffer);
                    break;
                case OpCode.BALANCE_COMMIT:
                    response = handleBalanceCommit(buffer);
                    break;
                case OpCode.STATS:
                    response = handleStatsRequest(buffer);
                    break;
                case OpCode.STATS_NO_NODE:
                    response = handleStatsNoNodeRequest(buffer);
                    break;
                case OpCode.QUALITY:
                    response = handleQualityRequest(buffer);
                    break;
                case OpCode.NODE_COUNT:
                    response = handleNodeCountRequest(buffer);
                    break;
                case OpCode.TO_STRING:
                    response = handleToStringRequest(buffer);
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

    private ByteBuffer handleNodeCountRequest(ByteBuffer buffer) {
        BaseRequest request = decoder.decodeBase(buffer);
        Response response = requestHandler.handleNodeCount(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleQualityRequest(ByteBuffer buffer) {
        BaseRequest request = decoder.decodeBase(buffer);
        Response response = requestHandler.handleQuality(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleStatsNoNodeRequest(ByteBuffer buffer) {
        BaseRequest request = decoder.decodeBase(buffer);
        Response response = requestHandler.handleStatsNoNode(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleToStringRequest(ByteBuffer buffer) {
        BaseRequest request = decoder.decodeBase(buffer);
        Response response = requestHandler.handleToString(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleStatsRequest(ByteBuffer buffer) {
        BaseRequest request = decoder.decodeBase(buffer);
        Response response = requestHandler.handleStats(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleBalanceCommit(ByteBuffer buffer) {
        CommitBalancingRequest request = decoder.decodeCommitBalancing(buffer);
        Response response = balancingRequestHandler.handleCommit(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleBalancePut(ByteBuffer buffer) {
        PutBalancingRequest<K> request = decoder.decodePutBalancing(buffer);
        Response response = balancingRequestHandler.handlePut(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleBalanceInit(ByteBuffer buffer) {
        InitBalancingRequest request = decoder.decodeInitBalancing(buffer);
        Response response = balancingRequestHandler.handleInit(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleContains(ByteBuffer buffer) {
        ContainsRequest<K> request = decoder.decodeContains(buffer);
        Response response = requestHandler.handleContains(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleCloseIterator(String clientHost, ByteBuffer buffer) {
        MapRequest request = decoder.decodeMap(buffer);
        Response response = requestHandler.handleCloseIterator(clientHost, request);
        return encodeResponse(response);
    }

    private ByteBuffer handleGetDimRequest(ByteBuffer buffer) {
        BaseRequest request = decoder.decodeBase(buffer);
        Response response = requestHandler.handleGetDim(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleGetDepthRequest(ByteBuffer buffer) {
        BaseRequest request = decoder.decodeBase(buffer);
        Response response = requestHandler.handleGetDepth(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleGetSizeRequest(ByteBuffer buffer) {
        BaseRequest request = decoder.decodeBase(buffer);
        Response response = requestHandler.handleGetSize(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleDeleteRequest(ByteBuffer buf) {
        DeleteRequest<K> request = decoder.decodeDelete(buf);
        Response response = requestHandler.handleDelete(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleGetBatchRequest(String clientHost, ByteBuffer buf) {
        GetIteratorBatchRequest<K> request = decoder.decodeGetBatch(buf);
        Response response = requestHandler.handleGetIteratorBatch(clientHost, request);
        return encodeResponse(response);
    }

    private ByteBuffer handleCreateRequest(ByteBuffer buf) {
        MapRequest request = decoder.decodeMap(buf);
        Response response = requestHandler.handleCreate(request);
        return encodeResponse(response);
    }

    private ByteBuffer handlePutRequest(ByteBuffer buf) {
        PutRequest<K, V> request = decoder.decodePut(buf);
        Response response = requestHandler.handlePut(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleGetRequest(ByteBuffer buf) {
        GetRequest<K> request = decoder.decodeGet(buf);
        Response response = requestHandler.handleGet(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleGetRangeRequest(ByteBuffer buf) {
        GetRangeRequest<K> request = decoder.decodeGetRange(buf);
        Response response = requestHandler.handleGetRange(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleGetKNNRequest(ByteBuffer buf) {
        GetKNNRequest<K> request = decoder.decodeGetKNN(buf);
        Response response = requestHandler.handleGetKNN(request);
        return encodeResponse(response);
    }

    private ByteBuffer handleErroneousRequest(ByteBuffer buf) {
        ResultResponse<K, V> response = new ResultResponse<>((byte) 0, 0, OpStatus.FAILURE);
        return encodeResponse(response);
    }

    private ByteBuffer encodeResponse(Response response) {
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