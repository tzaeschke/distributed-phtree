package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.operation.Response;
import ch.ethz.globis.distindex.api.IndexEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes response messages sent by the server to the client.
 *
 * @param <K>                   The type of the key.
 * @param <V>                   The type of the value.
 */
public class ByteResponseDecoder<K, V> implements ResponseDecoder<K, V> {

    FieldDecoder<K> keyDecoder;
    FieldDecoder<V> valueDecoder;

    public ByteResponseDecoder(FieldDecoder<V> valueDecoder) {
        this.valueDecoder = valueDecoder;
    }

    @Override
    public V decodePut(byte[] payload) {
        return valueDecoder.decode(payload);
    }

    public Response<K, V> decode(ByteBuffer buffer) {
        byte opCode = buffer.get();
        int requestId = buffer.get();
        byte status = buffer.get();
        int nrEntries = buffer.getInt();

        int keysBytesSize, valueBytesSize;
        byte[] keyBytes, valueBytes;
        K key;
        V value;
        List<IndexEntry<K, V>> entries = new ArrayList<>();
        for (int i = 0; i < nrEntries; i++) {
            keysBytesSize = buffer.getInt();
            keyBytes = new byte[keysBytesSize];
            buffer.get(keyBytes);
            key = keyDecoder.decode(keyBytes);

            valueBytesSize = buffer.getInt();
            valueBytes = new byte[valueBytesSize];
            buffer.get(valueBytes);
            value = valueDecoder.decode(valueBytes);

            entries.add(new IndexEntry<>(key, value));
        }

        return new Response<>(opCode, requestId, status, nrEntries, entries);
    }

    @Override
    public V decodeGet(byte[] payload) {
        return valueDecoder.decode(payload);
    }

    @Override
    public List<V> decodeGetRange(List<byte[]> payload) {
        throw new UnsupportedOperationException("Operation not yet implemented");
    }

    @Override
    public List<V> decodeGetKNN(List<byte[]> payload) {
        throw new UnsupportedOperationException("Operation not yet implemented");
    }

    @Override
    public boolean decodeCreate(byte[] payload) {
        return (payload.length == 1 && payload[0] == OpCode.SUCCESS);
    }
}
