package ch.ethz.globis.distindex.serializer;

import ch.ethz.globis.pht.PhTreeV;
import ch.ethz.globis.pht.v5.PhTree5;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PhTreeSerializerTest {

    private PhTreeSerializer serializer;

    public PhTreeSerializerTest(PhTreeSerializer serializer) {
        this.serializer = serializer;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { new FullTreeSerializer()}
        });
    }

    @Test
    public void exportPhTree() throws FileNotFoundException {
        int dim = 3;
        int size = 1000;
        PhTreeV<String> tree = createPhTree(size, dim);
        String filename = "tree.txt";

        serializer.export(tree, filename);
        PhTreeV<String> newTree = serializer.load(filename);

        assertEquals(tree.toStringPlain(), newTree.toStringPlain());
    }

    PhTreeV<String> createPhTree(int entries, int dim) {

        PhTreeV<String> tree = new PhTree5<>(dim, 64);

        long[] key;
        for (int i = 0; i < entries; i++) {
            key = createRandomKey(dim);
            tree.put(key, Arrays.toString(key));
        }
        return tree;
    }

    long[] createRandomKey(int dim) {
        Random random = new Random();
        long[] key = new long[dim];
        for (int i = 0; i < dim; i++) {
            key[i] = random.nextInt();
        }
        return key;
    }
}
