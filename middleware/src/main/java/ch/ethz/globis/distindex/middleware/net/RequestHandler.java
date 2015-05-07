package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.operation.request.BaseRequest;
import ch.ethz.globis.distindex.operation.request.ContainsRequest;
import ch.ethz.globis.distindex.operation.request.DeleteRequest;
import ch.ethz.globis.distindex.operation.request.GetIteratorBatchRequest;
import ch.ethz.globis.distindex.operation.request.GetKNNRequest;
import ch.ethz.globis.distindex.operation.request.GetRangeFilterMapperRequest;
import ch.ethz.globis.distindex.operation.request.GetRangeRequest;
import ch.ethz.globis.distindex.operation.request.GetRequest;
import ch.ethz.globis.distindex.operation.request.MapRequest;
import ch.ethz.globis.distindex.operation.request.PutRequest;
import ch.ethz.globis.distindex.operation.request.UpdateKeyRequest;
import ch.ethz.globis.distindex.operation.response.Response;

public interface RequestHandler<K, V> {

    public Response handleCreate(MapRequest request);

    public Response handleGet(GetRequest<K> request);

    public Response handleGetRange(GetRangeRequest<K> request);

    public Response handleGetKNN(GetKNNRequest<K> request);

    public Response handleGetIteratorBatch(String clientHost, GetIteratorBatchRequest<K> request);

    public Response handlePut(PutRequest<K, V> request);

    public Response handleDelete(DeleteRequest<K> request);

    public Response handleGetSize(BaseRequest request);

    public Response handleGetDim(BaseRequest request);

    public Response handleGetDepth(BaseRequest request);

    public Response handleCloseIterator(String clientHost, MapRequest request);

    public Response handleContains(ContainsRequest<K> request);

    public Response handleNodeCount(BaseRequest request);

    public Response handleQuality(BaseRequest request);

    public Response handleStatsNoNode(BaseRequest request);

    public Response handleToString(BaseRequest request);

    public Response handleStats(BaseRequest request);

    public void cleanup(String clientHost);

    public Response handleUpdateKey(UpdateKeyRequest<K> request);

    public Response handleGetRangeFilter(GetRangeFilterMapperRequest<K> request);
}