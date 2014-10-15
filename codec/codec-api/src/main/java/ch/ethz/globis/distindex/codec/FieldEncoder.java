package ch.ethz.globis.distindex.codec;

public interface FieldEncoder<V> {

    public byte[] encode(V value);
}
