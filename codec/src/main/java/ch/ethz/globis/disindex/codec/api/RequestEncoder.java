package ch.ethz.globis.disindex.codec.api;

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

    public byte[] encodePut(K key, V value);

    public byte[] encodeGet(K key);

    public byte[] encodeGetRange(K start, K end);

    public byte[] encodeGetKNN(K key, int k);

    public byte[] encodeCreate(int dim, int depth);
}
