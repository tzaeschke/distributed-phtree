package ch.ethz.globis.distindex.mapping.bst;

import ch.ethz.globis.distindex.mapping.KeyMapping;

import java.util.*;

public class BSTMapping<K> implements KeyMapping<K> {

    private BST<K> bst;
    private KeyConverter<K> converter;
    private List<String> intervals;

    public BSTMapping(KeyConverter<K> converter) {
        this.converter = converter;
        this.bst = new BST<K>();
        this.intervals = bst.leafs();
    }

    public BSTMapping(KeyConverter<K> converter, String[] hosts) {
        this.converter = converter;
        this.bst = BST.fromArray(hosts);
        this.intervals = bst.leafs();
    }

    @Override
    public Map<String, String> getHosts() {
        Map<String, String> codeHosts = new HashMap<>();
        getHosts("", bst.getRoot(), codeHosts);
        return codeHosts;
    }

    private void getHosts(String partial, BSTNode<K> node, Map<String, String> map) {
        if (node != null) {
            getHosts(partial + "0", node.getLeft(), map);
            if (node.getContent() != null) {
                map.put(partial, node.getContent());
            }
            getHosts(partial + "1", node.getRight(), map);
        }
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
    public String getFirst() {
        return intervals.get(0);
    }

    @Override
    public String getNext(String hostId) {
        int index = -1;
        for (int i = 0; i < intervals.size(); i++) {
            if (intervals.get(i).equals(hostId)) {
                index = i;
            }
        }
        if (index + 1 < intervals.size()) {
            return intervals.get(index + 1);
        } else {
            return null;
        }
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
