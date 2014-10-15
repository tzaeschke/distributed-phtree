package ch.ethz.globis.disindex.codec.field;

import ch.ethz.globis.distindex.codec.FieldEncoderDecoder;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class SerializingEncoderDecoder<V> implements FieldEncoderDecoder<V> {

    private final Kryo kryo;

    public SerializingEncoderDecoder() {
        this.kryo = new Kryo();
    }

    @Override
    public V decode(byte[] payload) {
        Input input = new Input(payload);
        kryo.readObject(input, Object.class);
        return null;
    }

    @Override
    public byte[] encode(V value) {
        return new byte[0];
    }
}
