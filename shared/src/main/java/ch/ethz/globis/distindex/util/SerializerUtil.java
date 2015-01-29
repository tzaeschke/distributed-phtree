package ch.ethz.globis.distindex.util;

import ch.ethz.globis.pht.PhPredicate;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.jboss.netty.handler.codec.serialization.ObjectEncoderOutputStream;
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

    public byte[] serializePhPredicate(PhPredicate predicate) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] data;
        try (ObjectOutput oo = new ObjectOutputStream(buffer)) {
            oo.writeObject(predicate);
            oo.flush();
            data = buffer.toByteArray();
        }
        return data;
    }

    public PhPredicate deserializePhPredicate(byte[] data) throws IOException, ClassNotFoundException {
        PhPredicate result;
        try (ObjectInput oi = new ObjectInputStream(new ByteArrayInputStream(data))) {
            result = (PhPredicate) oi.readObject();
        }
        return result;
    }
}