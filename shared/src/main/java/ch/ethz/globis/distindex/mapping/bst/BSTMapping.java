package ch.ethz.globis.distindex.mapping.bst;

import ch.ethz.globis.distindex.mapping.KeyMapping;

import java.util.ArrayList;
import java.util.List;

public class BSTMapping<K> implements KeyMapping<K> {

    private BST bst;
    private KeyConverter<K> converter;

    public BSTMapping(KeyConverter<K> converter) {
        this.converter = converter;
        this.bst = new BST();
    }

    @Override
    public String getHostId(K key) {
        BSTNode node = find(key);
        if (node == null) {
            return null;
        }
        return node.getContent();
    }

    @Override
    public List<String> getHostIds(K start, K end) {
        if (bst == null) {
            return new ArrayList<>();
        }
        return bst.leafs();
    }

    @Override
    public List<String> getHostIds() {
        if (bst == null) {
            return new ArrayList<>();
        }
        return bst.leafs();
    }

    @Override
    public void add(String host) {
        List<String> keys = bst.leafs();
        keys.add(host);
        bst = BST.fromArray(keys.toArray(new String[keys.size()]));
    }

    private BSTNode find(K key) {
        BSTNode current = bst.getRoot();
        if (current == null) {
            return null;
        }
        int position = 0;
        BSTNode previous = null;
        while (current != null) {
            previous = current;
            current = converter.isBitSet(key, position) ? current.getRight() : current.getLeft();
            position++;
        }
        return previous;
    }

}
