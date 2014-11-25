package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.operation.request.BalancingRequest;
import ch.ethz.globis.distindex.operation.request.CommitBalancingRequest;
import ch.ethz.globis.distindex.operation.request.InitBalancingRequest;
import ch.ethz.globis.distindex.operation.request.PutBalancingRequest;
import ch.ethz.globis.distindex.operation.response.Response;

public interface BalancingRequestHandler<K, V> {

    public Response handle(BalancingRequest request);

    public Response handleInit(InitBalancingRequest request);

    public Response handlePut(PutBalancingRequest<K> request);

    public Response handleCommit(CommitBalancingRequest request);
}
