package ch.ethz.globis.disindex.codec.api;

import ch.ethz.globis.distindex.operation.*;

/**
 * Contains operations corresponding to encoding requests send by the client library
 * to the middleware nodes..
 *
 * The request parameters are encoded as byte arrays.
 *
 * @param <K>                       The type of key.
 * @param <V>                       The type of value.
 */
public interface RequestEncoder<K, V> {

    public byte[] encodePut(PutRequest<K, V> request);

    public byte[] encodeGet(GetRequest<K> request);

    public byte[] encodeGetRange(GetRangeRequest<K> request);

    public byte[] encodeGetKNN(GetKNNRequest<K> request);

    public byte[] encodeCreate(CreateRequest request);
}
