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
        this.intervals = bst.leaves();
    }

    public BSTMapping(KeyConverter<K> converter) {
        this.converter = converter;
        this.bst = new BST<>();
    }

    @Override
    public void add(String host) {
        addHost(host);
    }

    private void addHost(String host) {
        Queue<BSTNode<K>> queue = new LinkedList<>();
        BSTNode<K> theNewNode = newNode(host);
        if (bst.getRoot() == null) {
            bst.setRoot(theNewNode);
        } else {
            boolean inserted = false;
            BSTNode<K> current;
            queue.add(bst.getRoot());
            while (!inserted) {
                current = queue.poll();
                if (addToNode(current, host)) {
                    inserted = true;
                } else {
                    addToQueue(queue, current.leftChild());
                    addToQueue(queue, current.rightChild());
                }
            }
            queue.clear();
        }
        this.intervals = bst.leaves();
    }

    private boolean addToNode(BSTNode<K> parent, String host) {
        if (parent.leftChild() == null && parent.rightChild() == null) {
            BSTNode<K> node = new BSTNode<>();
            node.setContent(parent.getContent());
            parent.setContent(null);
            parent.setLeft(node);
            parent.setRight(newNode(host));
            return true;
        }
        return false;
    }

    private BSTNode<K> newNode(String host) {
        BSTNode<K> current = new BSTNode<>();
        current.setContent(host);
        return current;
    }

    private boolean addToQueue(Queue<BSTNode<K>> queue, BSTNode<K> node) {
        if (node != null) {
            queue.add(node);
            return true;
        }
        return false;
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
            current = (bit == '0') ? current.leftChild() : current.rightChild();
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
        return bst.leaves();
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
    public Map<String, String> asMap() {
        Map<String, String> codeHosts = new HashMap<>();
        getHosts("", bst.getRoot(), codeHosts);
        return codeHosts;
    }

    private void getHosts(String partial, BSTNode<K> node, Map<String, String> map) {
        if (node != null) {
            getHosts(partial + "0", node.leftChild(), map);
            if (node.getContent() != null) {
                map.put(partial, node.getContent());
            }
            getHosts(partial + "1", node.rightChild(), map);
        }
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
        return Math.max(getDepth(hostId, node.leftChild(), depth + 1),
                getDepth(hostId, node.rightChild(), depth + 1) );

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
            current = converter.isBitSet(key, position) ? current.rightChild() : current.leftChild();
            position++;
        }
        return previous;
    }

}
