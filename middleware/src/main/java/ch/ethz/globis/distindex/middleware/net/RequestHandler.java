package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.operation.*;

public interface RequestHandler<K, V> {

    public Response<K, V> handleCreate(CreateRequest request);

    public Response<K, V> handleGet(GetRequest<K> request);

    public Response<K, V> handleGetRange(GetRangeRequest<K> request);

    public Response<K, V> handleGetKNN(GetKNNRequest<K> request);

    public Response<K, V> handleGetIteratorBatch(GetIteratorBatchRequest<K> request);

    public Response<K, V> handlePut(PutRequest<K, V> request);

    public Response<K,V> handleDelete(DeleteRequest<K> request);

    public IntegerResponse handleGetSize(Request request);

    public IntegerResponse handleGetDim(Request request);

    public IntegerResponse handleGetDepth(Request request);
}