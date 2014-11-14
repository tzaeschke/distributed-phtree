package ch.ethz.globis.distindex.mapping.bst;

import ch.ethz.globis.distindex.mapping.KeyMapping;

import java.util.*;

public class BSTMapping<K> implements KeyMapping<K> {

    private BST<K> bst;
    private KeyConverter<K> converter;
    private List<String> intervals;

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
        String prefix = converter.getBitPrefix(start, end);
        return getHostIds(prefix);
    }

    @Override
    public List<String> getHostIds(String prefix) {
        int prefixLength = prefix.length();
        BSTNode<K> current = bst.getRoot();
        int i = 0;
        String lastHost = null;
        while (current != null && i < prefixLength) {
            char bit = prefix.charAt(i++);
            lastHost = current.getContent();
            current = (bit == '0') ? current.getLeft() : current.getRight();
        }
        if (current == null) {
            List<String> results = new ArrayList<>();
            if (results != null) {
                results.add(lastHost);
            }
            return results;
        }
        return bst.getHosts(current);
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
    public Set<String> getHostsContaining(List<K> keys) {
        Set<String> neighbourHosts = new HashSet<>();
        for (K key : keys) {
            String hostId = getHostId(key);
            neighbourHosts.add(hostId);
        }
        return neighbourHosts;
    }

    @Override
    public int getDepth(String hostId) {
        return getDepth(hostId, bst.getRoot(), 0);
    }

    private int getDepth(String hostId, BSTNode<K> node, int depth) {
        if (node == null) {
            return 0;
        }
        if (node.getContent() != null && hostId.equals(node.getContent())) {
            return depth + 1;
        }
        return Math.max(getDepth(hostId, node.getLeft(), depth + 1),
                getDepth(hostId, node.getRight(), depth + 1) );

    }

    @Override
    public void add(String host) {
        List<String> keys = bst.leafs();
        keys.add(host);
        bst = BST.fromArray(keys.toArray(new String[keys.size()]));
    }

    private BSTNode<K> find(K key) {
        BSTNode<K> current = bst.getRoot();
        if (current == null) {
            return null;
        }
        int position = 0;
        BSTNode<K> previous = null;
        while (current != null) {
            previous = current;
            current = converter.isBitSet(key, position) ? current.getRight() : current.getLeft();
            position++;
        }
        return previous;
    }

}
