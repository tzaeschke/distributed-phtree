package ch.ethz.globis.distindex.mapping.bst;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class BSTTest {

    @Test
    public void testLeaves() {
        String[] keys = new String[]{ "one", "two", "three", "four", "five", "six", "seven" };
        BST bst = BST.fromArray(keys);
        assertEquals(Arrays.asList(keys), bst.leaves());
    }

    @Test
    public void testLargeCreation() {
        int size = 100;
        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = String.valueOf(i);
        }
        BST bst = BST.fromArray(keys);
        assertEquals(Arrays.asList(keys), bst.leaves());
    }

    @Test
    public void testGetNodes() {
        BST bst = new BST();
        int size = 100;
        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = String.valueOf(i);
            bst.add(keys[i]);
        }
        assertEquals(size, bst.nodes().size());
    }

    @Test
    public void testFindByContent() {
        BST bst = new BST();
        int size = 100;
        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = String.valueOf(i);
            bst.add(keys[i]);
        }

        for (int i = 0; i < size; i++) {
            BSTNode node = bst.findFirstByContent(String.valueOf(i));
            assertEquals(String.valueOf(i), node.getContent());
        }
    }
}