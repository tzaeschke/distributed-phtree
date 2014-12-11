package ch.ethz.globis.disindex.codec.field;

import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import ch.ethz.globis.distindex.util.SerializerUtil;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Generic Encoder - Decoder for any type of fields. The encoding/decoding is done using
 * serialization via Kryo.
 *
 * This encoder/decoder is only responsible for ONE type of Jave objects.
 *
 * @param <V>                               The type of Java objects used in the encoding/decoding.
 */
public class SerializingEncoderDecoder<V> implements FieldEncoderDecoder<V> {

    /** The class of the type of Java object,*/
    private Class<V> clazz;

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
    @SuppressWarnings("unchecked")
    public byte[] encode(V value) {
        if (value == null) {
            return new byte[0];
        }
        return SerializerUtil.getInstance().serialize(value);
    }
}