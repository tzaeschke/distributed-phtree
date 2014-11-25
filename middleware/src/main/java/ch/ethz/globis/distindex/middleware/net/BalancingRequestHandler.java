package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.operation.request.BalancingRequest;
import ch.ethz.globis.distindex.operation.response.Response;

public interface BalancingRequestHandler<K, V> {

    public Response handle(BalancingRequest request);
}
