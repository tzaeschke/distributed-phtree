package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.IntegerResponse;
import ch.ethz.globis.distindex.operation.response.ResultResponse;

public interface RequestHandler<K, V> {

    public ResultResponse<K, V> handleCreate(CreateRequest request);

    public ResultResponse<K, V> handleGet(GetRequest<K> request);

    public ResultResponse<K, V> handleGetRange(GetRangeRequest<K> request);

    public ResultResponse<K, V> handleGetKNN(GetKNNRequest<K> request);

    public ResultResponse<K, V> handleGetIteratorBatch(String clientHost, GetIteratorBatchRequest<K> request);

    public ResultResponse<K, V> handlePut(PutRequest<K, V> request);

    public ResultResponse<K,V> handleDelete(DeleteRequest<K> request);

    public IntegerResponse handleGetSize(BaseRequest request);

    public IntegerResponse handleGetDim(BaseRequest request);

    public IntegerResponse handleGetDepth(BaseRequest request);

    public IntegerResponse handleCloseIterator(String clientHost, MapRequest request);

    public void cleanup(String clientHost);
}