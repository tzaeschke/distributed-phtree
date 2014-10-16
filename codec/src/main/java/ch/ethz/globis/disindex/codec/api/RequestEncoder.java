package ch.ethz.globis.disindex.codec.api;


public interface RequestEncoder<K, V> {

    public byte[] encodePut(K key, V value);

    public byte[] encodeGet(K key);

    public byte[] encodeGetRange(K start, K end);

    public byte[] encodeGetKNN(K key, int k);
}
