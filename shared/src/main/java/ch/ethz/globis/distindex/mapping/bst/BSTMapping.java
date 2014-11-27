package ch.ethz.globis.distindex.mapping.bst;

import ch.ethz.globis.distindex.mapping.KeyMapping;

import java.util.*;

public class BSTMapping<K> implements KeyMapping<K> {

    private BST bst;
    private KeyConverter<K> converter;
    private List<String> intervals;

    public BSTMapping() {
        //this should not be used, but is needed for Kryo
    }

    public BSTMapping(KeyConverter<K> converter, String[] hosts) {
        this.converter = converter;
        this.bst = BST.fromArray(hosts);
        this.intervals = bst.leaves();
    }

    public BSTMapping(KeyConverter<K> converter) {
        this.converter = converter;
        this.bst = new BST();
    }

    @Override
    public void add(String host) {
        this.bst.add(host);
        this.intervals = bst.leaves();
    }

    @Override
    public void remove(String host) {
        bst.setRoot(remove(bst.getRoot(), host));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void split(String splittingHostId, String receiverHostId, int sizeMoved) {
        //ToDo split the zone with the largest size
        BSTNode splitting = bst.findFirstByContent(splittingHostId);
        int previousSize = splitting.getSize();
        bst.addToNode(splitting, receiverHostId);
        BSTNode oldZone = splitting.leftChild();
        oldZone.setSize(previousSize - sizeMoved);

        BSTNode newZone = splitting.rightChild();
        newZone.setSize(sizeMoved);
    }

    @Override
    public void setSize(String host, int size) {
        BSTNode node = bst.findFirstByContent(host);
        node.setSize(size);
    }

    @Override
    public String getHostForSplitting() {
        List<BSTNode> nodes = bst.nodes();
        Collections.sort(nodes, new Comparator<BSTNode>() {
            @Override
            public int compare(BSTNode o1, BSTNode o2) {
                int size1 = o1.getSize();
                int size2 = o2.getSize();
                if (size1 == size2) {
                    return 0;
                } else {
                    if (size1 < size2) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            }
        });
        if (nodes.size() > 0) {
            return nodes.get(0).getContent();
        }
        return null;
    }

    @Override
    public int size() {
        //ToDo implement this properly
        return bst.leaves().size();
    }

    @Override
    public void clear() {
        this.bst = new BST();
        this.intervals = bst.leaves();
    }

    @Override
    public String getLargestZone(String currentHostId) {
        List<BSTNode> nodes = bst.findByContent(currentHostId);
        int maxSize = 0;
        BSTNode largest = null;
        for (BSTNode node : nodes) {
            if (node.getSize() > maxSize) {
                largest = node;
                maxSize = node.getSize();
            }
        }
        return (largest == null) ? null : largest.getPrefix();
    }

    private BSTNode remove(BSTNode node, String host) {
        if (node.getContent() != null) {
            if (host.equals(node.getContent())) {
                return null;
            } else {
                return node;
            }
        }
        BSTNode left = remove(node.leftChild(), host);
        BSTNode right = remove(node.rightChild(), host);
        BSTNode returned;
        if (left == null) {
            returned = new BSTNode(right);
            returned.setPrefix(node.getPrefix());
            return returned;
        }
        if (right == null) {
            returned = new BSTNode(left);
            returned.setPrefix(node.getPrefix());
            return returned;
        }
        node.setLeft(left);
        node.setRight(right);
        return node;
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
        BSTNode current = bst.getRoot();
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

    private void getHosts(String partial, BSTNode node, Map<String, String> map) {
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

    private int getDepth(String hostId, BSTNode node, int depth) {
        if (node == null) {
            return 0;
        }
        if (node.getContent() != null && hostId.equals(node.getContent())) {
            return depth + 1;
        }
        return Math.max(getDepth(hostId, node.leftChild(), depth + 1),
                getDepth(hostId, node.rightChild(), depth + 1) );

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
            current = converter.isBitSet(key, position) ? current.rightChild() : current.leftChild();
            position++;
        }
        return previous;
    }

    public BST getBst() {
        return bst;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BSTMapping)) return false;

        BSTMapping that = (BSTMapping) o;

        if (bst != null ? !bst.equals(that.bst) : that.bst != null) return false;
        if (converter != null ? !converter.equals(that.converter) : that.converter != null) return false;
        if (intervals != null ? !intervals.equals(that.intervals) : that.intervals != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = bst != null ? bst.hashCode() : 0;
        result = 31 * result + (converter != null ? converter.hashCode() : 0);
        result = 31 * result + (intervals != null ? intervals.hashCode() : 0);
        return result;
    }
}