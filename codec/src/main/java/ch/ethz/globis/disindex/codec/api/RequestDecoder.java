package ch.ethz.globis.disindex.codec.api;

import ch.ethz.globis.distindex.operation.request.*;

import java.nio.ByteBuffer;

/**
 * Contains operations corresponding to decoding requests send by the client library.
 *
 * The request parameters are encoded as byte arrays.
 *
 * @param <K>                       The type of key.
 * @param <V>                       The type of value.
 */
public interface RequestDecoder<K, V> {

    public GetRequest<K> decodeGet(ByteBuffer buffer);

    public ContainsRequest<K> decodeContains(ByteBuffer buffer);

    public PutRequest<K, V> decodePut(ByteBuffer buffer);

    public GetRangeRequest<K> decodeGetRange(ByteBuffer buffer);

    public GetKNNRequest<K> decodeGetKNN(ByteBuffer buffer);

    public GetIteratorBatchRequest<K> decodeGetBatch(ByteBuffer buffer);

    public CreateRequest decodeCreate(ByteBuffer buffer);

    public DeleteRequest<K> decodeDelete(ByteBuffer buffer);

    public BaseRequest decodeBase(ByteBuffer buffer);

    public MapRequest decodeMap(ByteBuffer buffer);

    public InitBalancingRequest decodeInitBalancing(ByteBuffer buffer);

    public PutBalancingRequest<K> decodePutBalancing(ByteBuffer buffer);

    public CommitBalancingRequest decodeCommitBalancing(ByteBuffer buffer);
}
