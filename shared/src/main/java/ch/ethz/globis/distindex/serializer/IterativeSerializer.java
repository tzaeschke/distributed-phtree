package ch.ethz.globis.distindex.serializer;

import ch.ethz.globis.pht.PVEntry;
import ch.ethz.globis.pht.PVIterator;
import ch.ethz.globis.pht.PhTreeV;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.*;

public class IterativeSerializer<T> implements PhTreeSerializer {

    private PhTreeV<T> tree;

    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).
                    setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            return kryo;
        };
    };

    @Override
    public <T> void export(PhTreeV<T> tree, String filename) throws FileNotFoundException {
        try (Output output = createOutput(filename)) {
            Kryo kryo = kryos.get();
            PVIterator<T> it = tree.queryExtent();
            while (it.hasNext()) {
                kryo.writeClassAndObject(output, it.nextEntry());
            }
            output.flush();
        }
    }

    private Output createOutput(String filename) throws FileNotFoundException {
        return new Output(new BufferedOutputStream(new FileOutputStream(filename)));
    }

    @Override
    public PhTreeV<T> load(String filename) throws FileNotFoundException {
        try (Input input = new Input(new BufferedInputStream(new FileInputStream(filename)))){
            Kryo kryo = kryos.get();
            PVEntry<T> e;
            while (!input.eof()) {
                e = (PVEntry<T>) kryo.readClassAndObject(input);
                tree.put(e.getKey(), e.getValue());
            }
            return tree;
        }
    }

    public void setTree(PhTreeV<T> tree) {
        this.tree = tree;
    }

    public PhTreeV<T> getTree() {
        return tree;
    }
}
