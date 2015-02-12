package ch.ethz.globis.distindex.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.*;

public class SerializerUtil {

    private static final SerializerUtil serializer = new SerializerUtil();

    // Setup ThreadLocal of Kryo instances
    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).
                    setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            return kryo;
        };
    };

    public static SerializerUtil getInstance() {
        return serializer;
    }

    public <T> byte[] serialize(T object) {
        ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
        Output output = new Output(outputBuffer);

        serialize(object, output);

        byte[] data =  outputBuffer.toByteArray();
        output.close();
        return data;
    }

    public <T> void serialize(T object, Output output) {
        Kryo kryo = kryos.get();
        kryo.writeClassAndObject(output, object);
        output.flush();
    }

    public <T> T deserialize(byte[] data) {
        Kryo kryo = kryos.get();
        Input input = new Input(data);
        T obj = (T) kryo.readClassAndObject(input);
        input.close();
        return obj;
    }

    public byte[] serializeDefault(Serializable object) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data;
        try (ObjectOutput oo = new ObjectOutputStream(buffer)) {
            oo.writeObject(object);
            oo.flush();
            data = buffer.toByteArray();
        }
        return data;
    }

    public <T> T deserializeDefault(byte[] data) throws IOException, ClassNotFoundException {
        T result;
        try (ObjectInput oi = new ObjectInputStream(new ByteArrayInputStream(data))) {
            result = (T) oi.readObject();
        }
        return result;
    }
}