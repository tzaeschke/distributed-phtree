package ch.ethz.globis.disindex.codec;

import ch.ethz.globis.disindex.codec.api.FieldDecoder;
import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.util.Pair;

import java.nio.ByteBuffer;

/**
 * Decodes request messages from the client.
 *
 * @param <K>                   The type of the key.
 */
public class ByteRequestDecoder<K> implements RequestDecoder<K, byte[]> {

    /** The decoder used for the key.*/
    private FieldDecoder<K> keyDecoder;

    public ByteRequestDecoder(FieldDecoder<K> keyDecoder) {
        this.keyDecoder = keyDecoder;
    }

    /**
     * Decode a get request from the client.
     *
     * @param buffer                    The ByteBuffer containing bytes sent by the client.
     *                                  The request type has already been read.
     * @return                          The key for which the get operation has to be executed.
     */
    @Override
    public K decodeGet(ByteBuffer buffer) {
        int keyBytesSize = buffer.getInt();
        byte[] keyBytes = new byte[keyBytesSize];
        buffer.get(keyBytes);

        assert !buffer.hasRemaining();

        return keyDecoder.decode(keyBytes);
    }

    /**
     * Decode a put request from the client.
     * @param buffer                    The ByteBuffer containing bytes sent by the client.
     *                                  The request type has already been read.
     * @return                          The key and value that have to be added to the index.
     */
    @Override
    public Pair<K, byte[]> decodePut(ByteBuffer buffer) {
        K key = decodeKey(buffer);
        byte[] value = decodeValue(buffer);
        return new Pair<>(key, value);
    }

    /**
     * Decode a get values in range request from the client.
     * @param buffer                    The ByteBuffer containing bytes sent by the client.
     *                                  The request type has already been read.
     * @return                          The keys containing the range for the request.
     */
    @Override
    public Pair<K, K> decodeGetRange(ByteBuffer buffer) {
        K start = decodeKey(buffer);
        K end = decodeKey(buffer);
        return new Pair<>(start, end);
    }

    /**
     * Decode a k nearest neighbor request from the client.
     * @param buffer                    The ByteBuffer containing bytes sent by the client.
     *                                  The request type has already been read.
     * @return                          The key for which the search should be done and the number
     *                                  of neighbours.
     */
    @Override
    public Pair<K, Integer> decodeGetKNN(ByteBuffer buffer) {
        K key = decodeKey(buffer);
        Integer k = buffer.getInt();
        return new Pair<>(key, k);
    }

    @Override
    public Pair<Integer, Integer> decodeCreate(ByteBuffer buffer) {
        int dim = buffer.getInt();
        int depth = buffer.getInt();
        return new Pair<>(dim, depth);
    }

    /**
     * Decode a key from the current ByteBuffer.
     *
     * The decoding is performed in the following steps:
     *  - the integer size of the key byte array is read from the buffer.
     *  - the byte array containing the key is then read.
     *  - the key is decoded using the keyDecoder.
     * @param buffer                        The ByteBuffer containing the key.
     * @return                              The decoded key.
     */
    private K decodeKey(ByteBuffer buffer) {
        int keyBytesSize = buffer.getInt();
        byte[] keyBytes = new byte[keyBytesSize];
        buffer.get(keyBytes);
        return keyDecoder.decode(keyBytes);
    }

    /**
     * Decode a value from the current ByteBuffer.
     *
     * The decoding is performed in the following steps:
     *  - the integer size of the value byte array is read from the buffer.
     *  - the byte array containing the value is then read.
     *  - the value is decoded using the valueDecoder.
     * @param buffer                        The ByteBuffer containing the key.
     * @return                              The decoded key.
     */
    private byte[] decodeValue(ByteBuffer buffer) {
        int valueBytesSize = buffer.getInt();
        byte[] valueBytes = new byte[valueBytesSize];
        buffer.get(valueBytes);
        return valueBytes;
    }
}