package ch.ethz.globis.disindex.codec.api;

import java.util.List;

/**
 * Contains operations corresponding to encoding responses send by the middleware node to the client library.
 *
 * The response parameters are encoded as byte arrays.
 *
 * @param <K>                       The type of key.
 * @param <V>                       The type of value.
 */
public interface ResponseEncoder<K, V> {

    public byte[] encodePut(K key, V value);

    public byte[] encodeGet(V value);

    public byte[] encodeGetRange(List<V> values);

    public byte[] encodeGetKNN(List<V> values);
}
