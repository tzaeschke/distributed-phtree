package ch.ethz.globis.distindex.serializer;

import ch.ethz.globis.pht.PhTree;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.*;

public class FullTreeSerializer implements PhTreeSerializer {

    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).
                    setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            return kryo;
        };
    };

    @Override
    public <T> void export(PhTree<T> tree, String filename) throws FileNotFoundException {
        try (Output output = createOutput(filename)) {
            Kryo kryo = kryos.get();
            kryo.writeClassAndObject(output, tree);
            output.flush();
        }
    }

    @SuppressWarnings("unchecked")
	@Override
    public <T> PhTree<T> load(String filename) throws FileNotFoundException {
        try (Input input = new Input(new BufferedInputStream(new FileInputStream(filename)))){
            Kryo kryo = kryos.get();
            return (PhTree<T>) kryo.readClassAndObject(input);
        }
    }

    private Output createOutput(String filename) throws FileNotFoundException {
        return new Output(new BufferedOutputStream(new FileOutputStream(filename)));
    }
}