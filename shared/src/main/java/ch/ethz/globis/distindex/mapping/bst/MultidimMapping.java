package ch.ethz.globis.distindex.mapping.bst;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;

public class MultidimMapping extends BSTMapping<long[]> {

    public MultidimMapping() {
        super(new LongArrayKeyConverter());
    }

    public byte[] serialize() {
        Kryo kryo = new Kryo();
        Output output = new Output(new ByteArrayOutputStream());
        kryo.writeClassAndObject(output, this);
        return output.toBytes();
    }

    public static MultidimMapping deserialize(byte[] data) {
        Kryo kryo = new Kryo();
        return (MultidimMapping) kryo.readClassAndObject(new Input(data));
    }
}
