package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.Response;

public interface BalancingRequestHandler<K> {


    public Response handleInit(InitBalancingRequest request);

    public Response handlePut(PutBalancingRequest<K> request);

    public Response handleCommit(CommitBalancingRequest request);

    public Response handleRollback(RollbackBalancingRequest request);
}
