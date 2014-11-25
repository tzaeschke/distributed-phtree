package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.distindex.middleware.net.BalancingRequestHandler;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.operation.request.BalancingRequest;
import ch.ethz.globis.distindex.operation.response.Response;

public class PhTreeBalancingRequestHandler implements BalancingRequestHandler<long[], byte[]> {

    private IndexContext indexContext;

    public PhTreeBalancingRequestHandler(IndexContext indexContext) {
        this.indexContext = indexContext;
    }

    @Override
    public Response handle(BalancingRequest request) {
        byte opCode = request.getOpCode();
        switch (opCode) {
            case OpCode.BALANCE_INIT:
                break;
            case OpCode.BALANCE_PUT:
                break;
            case OpCode.BALANCE_COMMIT:
                break;
            default:
                break;
        }
        return null;
    }
}