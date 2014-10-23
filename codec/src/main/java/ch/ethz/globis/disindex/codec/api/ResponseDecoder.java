package ch.ethz.globis.disindex.codec.api;

import java.util.List;

/**
 * Contains operations corresponding to decoding responses send by the middleware node to the client library.
 *
 * The response parameters are encoded as byte arrays.
 *
 * @param <K>                       The type of key.
 * @param <V>                       The type of value.
 */
public interface ResponseDecoder<K, V> {

    public V decodePut(byte[] payload);

    public V decodeGet(byte[] payload);

    public List<V> decodeGetRange(List<byte[]> payload);

    public List<V> decodeGetKNN(List<byte[]> payload);

    public boolean decodeCreate(byte[] payload);
}
