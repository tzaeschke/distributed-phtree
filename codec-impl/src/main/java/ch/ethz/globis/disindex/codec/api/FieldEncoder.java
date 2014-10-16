package ch.ethz.globis.disindex.codec.api;

public interface FieldEncoder<V> {

    public byte[] encode(V value);
}
