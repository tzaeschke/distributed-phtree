package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.operation.Response;

import java.nio.ByteBuffer;

/**
 * Decodes response messages sent by the server to the client.
 *
 * @param <K>                   The type of the key.
 * @param <V>                   The type of the value.
 */
public class ByteResponseDecoder<K, V> implements ResponseDecoder<K, V> {

    FieldDecoder<K> keyDecoder;
    FieldDecoder<V> valueDecoder;

    public ByteResponseDecoder(FieldDecoder<K> keyDecoder, FieldDecoder<V> valueDecoder) {
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
    }

    public Response<K, V> decode(ByteBuffer buffer) {
        byte opCode = buffer.get();
        int requestId = buffer.getInt();
        byte status = buffer.get();
        int nrEntries = buffer.getInt();

        int keysBytesSize, valueBytesSize;
        byte[] keyBytes, valueBytes;
        K key;
        V value;
        IndexEntryList<K, V> entries = new IndexEntryList<>();
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

        return new Response<>(opCode, requestId, status, entries);
    }

    @Override
    public Response<K, V> decode(byte[] payload) {
        return decode(ByteBuffer.wrap(payload));
    }
}
