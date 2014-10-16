package ch.ethz.globis.disindex.codec.field;

import ch.ethz.globis.distindex.codec.FieldEncoderDecoder;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SerializingEncoderDecoder<V> implements FieldEncoderDecoder<V> {

    private final Kryo kryo;
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
    public byte[] encode(V value) {
        Output output = new Output();
        kryo.writeObject(output, value);
        return output.getBuffer();
    }
}