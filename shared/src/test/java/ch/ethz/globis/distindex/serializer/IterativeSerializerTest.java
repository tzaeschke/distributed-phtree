package ch.ethz.globis.distindex.serializer;

import ch.ethz.globis.distindex.mapping.util.TestOperationsUtil;
import ch.ethz.globis.pht.PhTreeV;
import ch.ethz.globis.pht.v5.PhTree5;
import org.junit.Test;

import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;

public class IterativeSerializerTest {

    @Test
    public void exportPhTree() throws FileNotFoundException {
        int dim = 6;
        int size = 100000;
        PhTreeV<String> tree = TestOperationsUtil.createRandomPhTree(size, dim);
        String filename = "tree.txt";
        IterativeSerializer<String> serializer = new IterativeSerializer<>();

        serializer.export(tree, filename);

        PhTreeV<String> outputTree = new PhTree5<>(dim, 64);
        serializer.setTree(outputTree);

        PhTreeV<String> newTree = serializer.load(filename);

        assertEquals(tree.toStringPlain(), newTree.toStringPlain());
    }
}
