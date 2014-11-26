package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.middleware.net.BalancingRequestHandler;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.BaseResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.pht.PhTreeV;

public class PhTreeBalancingRequestHandler implements BalancingRequestHandler<long[], byte[]> {

    private IndexContext indexContext;
    private IndexEntryList<long[], byte[]> buffer;

    public PhTreeBalancingRequestHandler(IndexContext indexContext) {
        this.indexContext = indexContext;
    }

    @Override
    public Response handle(BalancingRequest request) {
        byte opCode = request.getOpCode();
        Response response;
        switch (opCode) {
            case OpCode.BALANCE_INIT:
                response = handleInit((InitBalancingRequest) request);
                break;
            case OpCode.BALANCE_PUT:
                response = handlePut((PutBalancingRequest<long[]>) request);
                break;
            case OpCode.BALANCE_COMMIT:
                response = handleCommit((CommitBalancingRequest) request);
                break;
            default:
                response = null;
                break;
        }
        return response;
    }

    @Override
    public Response handleInit(InitBalancingRequest request) {
        int size = request.getSize();
        buffer = new IndexEntryList<>(size);

        return ackResponse(request);
    }

    @Override
    public Response handlePut(PutBalancingRequest<long[]> request) {
        long[] key = request.getKey();
        byte[] value = request.getValue();
        buffer.add(key, value);

        return ackResponse(request);
    }

    @Override
    public Response handleCommit(CommitBalancingRequest request) {
        PhTreeV<byte[]> tree = indexContext.getTree();
        synchronized (tree) {
            for (IndexEntry<long[], byte[]> entry : buffer) {
                tree.put(entry.getKey(), entry.getValue());
            }
        }
        return ackResponse(request);
    }

    private Response ackResponse(Request request) {
        return new BaseResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS);
    }
}