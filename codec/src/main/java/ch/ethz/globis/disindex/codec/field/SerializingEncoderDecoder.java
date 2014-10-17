package ch.ethz.globis.disindex.codec.field;

import ch.ethz.globis.disindex.codec.api.FieldEncoderDecoder;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.FastOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

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

    /** Reference to the Kryo API */
    private final Kryo kryo;

    /** The class of the type of Java object,*/
    private final Class<V> clazz;

    public SerializingEncoderDecoder(Class<V> clazz) {
        this.clazz = clazz;
        this.kryo = new Kryo();
        kryo.register(clazz);
    }

    @Override
    public V decode(byte[] payload) {
        Input input = new Input(payload);
        return kryo.readObject(input, clazz);
    }

    @Override
    public V decode(ByteBuffer payload) {
        return decode(payload.array());
    }

    @Override
    public byte[] encode(V value) {
        //ToDo make the serializing buffer configurable.
        Output output = new Output(1024);
        kryo.writeObject(output, value);
        int position = output.position();

        return Arrays.copyOf(output.getBuffer(), position);
    }
}