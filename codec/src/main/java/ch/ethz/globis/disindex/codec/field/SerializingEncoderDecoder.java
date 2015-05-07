package ch.ethz.globis.disindex.codec.field;

import java.nio.ByteBuffer;

import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.distindex.util.SerializerUtil;

/**
 * Generic Encoder - Decoder for any type of fields. The encoding/decoding is done using
 * serialization via Kryo.
 *
 * This encoder/decoder is only responsible for ONE type of Jave objects.
 *
 * @param <V>                               The type of Java objects used in the encoding/decoding.
 */
public class SerializingEncoderDecoder<V> implements FieldEncoderDecoder<V> {

    public SerializingEncoderDecoder() {
    }

    @Override
    public V decode(byte[] payload) {
        if (payload.length == 0) {
            return null;
        }
        return SerializerUtil.getInstance().deserialize(payload);
    }

    @Override
    public V decode(ByteBuffer payload) {
        return decode(payload.array());
    }

    @Override
    public byte[] encode(V value) {
        if (value == null) {
            return new byte[0];
        }
        return SerializerUtil.getInstance().serialize(value);
    }
}