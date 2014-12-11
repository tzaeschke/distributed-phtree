package ch.ethz.globis.distindex.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;

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
        Kryo kryo = kryos.get();
        ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();
        Output output = new Output(outputBuffer);
        kryo.writeClassAndObject(output, object);
        output.flush();
        byte[] data =  outputBuffer.toByteArray();
        output.close();
        return data;
    }

    public <T> T deserialize(byte[] data) {
        Kryo kryo = kryos.get();
        Input input = new Input(data);
        T obj = (T) kryo.readClassAndObject(input);
        input.close();
        return obj;
    }
}