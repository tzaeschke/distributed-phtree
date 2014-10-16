package ch.ethz.globis.disindex.codec.api;

import java.util.List;

public interface ResponseDecoder<K, V> {

    public V decodePut(byte[] payload);

    public V decodeGet(byte[] payload);

    public List<V> decodeGetRange(List<byte[]> payload);

    public List<V> decodeGetKNN(List<byte[]> payload);
}
