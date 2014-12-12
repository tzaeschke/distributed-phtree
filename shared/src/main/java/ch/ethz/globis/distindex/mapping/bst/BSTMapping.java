package ch.ethz.globis.distindex.mapping.bst;

import ch.ethz.globis.distindex.mapping.KeyMapping;

import java.util.*;

public class BSTMapping<K> implements KeyMapping<K> {

    private BST bst;
    private KeyConverter<K> converter;
    private List<String> intervals;
    private int version = 0;

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

    /**
     * Add a hostId to the key mapping.
     * @param host
     */
    @Override
    public void add(String host) {
        this.bst.add(host);
        this.intervals = bst.leaves();
    }

    /**
     * Remove a hostId from the keyMapping
     * @param host
     */
    @Override
    public void remove(String host) {
        bst.setRoot(remove(bst.getRoot(), host));
    }

    /**
     * Split the area available to a hostId and attribute half of it to a receiving hostId.
     *
     * The number of entries moved is sizeMoved.
     *
     * @param splittingHostId                       The hostId that will be split.
     * @param receiverHostId                        The hostId that will received the entries moved.
     * @param sizeMoved                             The number of entries to be moved
     */
    @SuppressWarnings("unchecked")
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

    /**
     * Set the number of keys associated with a host.
     *
     * @param host
     * @param size
     */
    @Override
    public void setSize(String host, int size) {
        BSTNode node = bst.findFirstByContent(host);
        node.setSize(size);
    }

    @Override
    public int getSize(String host) {
        BSTNode node = bst.findFirstByContent(host);
        return node.getSize();
    }

    /**
     * Return the host that can be the receiver of a split operation.
     *
     * Should not necessarily bet the host with the smallest number of keys, as that host could be currently
     * part of a running re-balancing.
     *
     * @return
     */
    public String getHostForSplitting(String currentHost) {
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
        String hostId = null;
        if (nodes.size() > 0) {
            hostId = nodes.get(0).getContent();
            if (hostId.equals(currentHost)) {
                if (nodes.size() > 1) {
                    hostId = nodes.get(1).getContent();
                } else {
                    hostId = null;
                }
            }
        }
        return hostId;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    /**
     * Return the number of hosts within the mapping.
     * @return
     */
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

    /**
     * Get the zone that has the largest size from the zones owned by the host whose hostId is
     * received as an argument.
     *
     * @param currentHostId                             The hostId
     * @return
     */
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
        return (largest == null) ? "" : largest.getPrefix();
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


    /**
     * Obtain the hostId of the machine that stores the key received as an argument.
     *
     * @param key
     * @return
     */
    @Override
    public String get(K key) {
        BSTNode node = find(key);
        if (node == null) {
            return null;
        }
        return node.getContent();
    }

    /**
     * Obtain all hostIds that store keys between the range determined by the start and end keys
     * received as arguments.
     * @param start
     * @param end
     * @return
     */
    @Override
    public List<String> get(K start, K end) {
        if (bst == null) {
            return new ArrayList<>();
        }
        String prefix = converter.getBitPrefix(start, end);
        return getHostIds(prefix);
    }

    /**
     * Get all the host id's which holds keys having the bit prefix received as an argument.
     * @param prefix
     * @return
     */
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


    /**
     * Obtain all the host ids.
     * @return
     */
    @Override
    public List<String> get() {
        if (bst == null) {
            return new ArrayList<>();
        }
        return bst.leaves();
    }


    /**
     * Get the hostId of the host that holds the first key interval.
     * @return
     */
    @Override
    public String getFirst() {
        return intervals.get(0);
    }

    /**
     * Get the hostUId of the host that holds the next key interval relative to the key interval
     * of the hostId received as an argument.
     * @param hostId
     * @return
     */
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
    public String getPrevious(String hostId) {
        int index = -1;
        for (int i = 0; i < intervals.size(); i++) {
            if (intervals.get(i).equals(hostId)) {
                index = i;
            }
        }
        if (index - 1 >= 0) {
            return intervals.get(index + 1);
        } else {
            return null;
        }
    }

    /**
     * Return all of the host ids that contain the keys received as arguments.
     * @param keys
     * @return
     */
    public Set<String> getHostsContaining(List<K> keys) {
        Set<String> neighbourHosts = new HashSet<>();
        for (K key : keys) {
            String hostId = get(key);
            neighbourHosts.add(hostId);
        }
        return neighbourHosts;
    }

    /**
     * @return                  the prefix - host mapping.
     */
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

    /**
     * Get the depth of the hostId.
     * @param hostId
     * @return
     */
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