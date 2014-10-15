package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.distindex.codec.ResponseDecoder;

import java.util.List;

/**
 * Created by bvancea on 15.10.14.
 */
public class ByteResponseDecoder<K, V> implements ResponseDecoder<K, V> {

    @Override
    public V decodePut(byte[] payload) {
        return null;
    }

    @Override
    public V decodeGet(byte[] payload) {
        return null;
    }

    @Override
    public List<V> decodeGetRange(List<byte[]> payload) {
        return null;
    }

    @Override
    public List<V> decodeGetKNN(List<byte[]> payload) {
        return null;
    }
}
