package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.operation.*;

public interface RequestHandler<K, V> {

    public Response<K, V> handleCreate(CreateRequest request);

    public Response<K, V> handleGet(GetRequest<K> request);

    public Response<K, V> handleGetRange(GetRangeRequest<K> request);

    public Response<K, V> handleGetKNN(GetKNNRequest<K> request);

    public Response<K, V> handleGetIteratorBatch(GetIteratorBatch<K> request);

    public Response<K, V> handlePut(PutRequest<K, V> request);

}