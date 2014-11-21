package ch.ethz.globis.distindex.mapping.bst;

import java.util.ArrayList;
import java.util.List;

public class BST<K> {

    private BSTNode<K> root;

    public BSTNode getRoot() {
        return root;
    }

    public void setRoot(BSTNode root) {
        this.root = root;
    }

    public static <K> BST<K> fromArray(String[] array) {
        BSTNode node = fromArray(array, 0, array.length - 1);
        BST<K> bst =  new BST<>();
        bst.setRoot(node);
        return bst;
    }

    private static <K> BSTNode fromArray(String[] array, int start, int end) {
        if (start > end) {
            return null;
        }
        if (start == end) {
            BSTNode<K> current = new BSTNode<>();
            current.setContent(array[start]);
            return current;
        }
        int mid = start + (end - start) / 2;
        BSTNode<K> current = new BSTNode<>();
        current.setLeft(fromArray(array, start, mid));
        current.setRight(fromArray(array, mid + 1, end));
        return current;
    }

    public List<String> getHosts(BSTNode<K> node) {
        List<String> hosts = new ArrayList<>();
        getHosts(node, hosts);
        return hosts;
    }

    private void getHosts(BSTNode<K> node, List<String> hosts) {
        if (node != null) {
            getHosts(node.leftChild(), hosts);
            if (node.getContent() != null) {
                hosts.add(node.getContent());
            }
            getHosts(node.rightChild(), hosts);
        }
    }

    public List<String> leaves() {
        List<String> results = new ArrayList<>();
        findRange(root, results);
        return results;
    }

    private void findRange(BSTNode<K> current, List<String> result) {
        if (current == null) {
            return;
        }
        findRange(current.leftChild(), result);
        if (current.getContent() != null) {
            result.add(current.getContent());
        }
        findRange(current.rightChild(), result);
    }
}