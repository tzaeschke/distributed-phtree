package ch.ethz.globis.disindex.codec.api;

public interface FieldDecoder<V> {

    public V decode(byte[] payload);
}
