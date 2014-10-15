package ch.ethz.globis.distindex.codec;

public interface FieldDecoder<V> {

    public V decode(byte[] payload);
}
