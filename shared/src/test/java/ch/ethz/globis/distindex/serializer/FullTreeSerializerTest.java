package ch.ethz.globis.distindex.serializer;

import ch.ethz.globis.distindex.mapping.util.TestOperationsUtil;
import ch.ethz.globis.pht.PhTreeV;
import org.junit.Test;

import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;

public class FullTreeSerializerTest {

    @Test
    public void exportPhTree() throws FileNotFoundException {
        int dim = 6;
        int size = 100000;
        PhTreeV<String> tree = TestOperationsUtil.createRandomPhTree(size, dim);
        String filename = "tree.txt";
        FullTreeSerializer serializer = new FullTreeSerializer();

        serializer.export(tree, filename);
        PhTreeV<String> newTree = serializer.load(filename);

        assertEquals(tree.toStringPlain(), newTree.toStringPlain());
    }
}