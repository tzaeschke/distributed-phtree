package ch.ethz.globis.distindex.mapping.bst;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BST<K> {

    private BSTNode<K> root;

    public BSTNode getRoot() {
        return root;
    }

    public void setRoot(BSTNode root) {
        this.root = root;
    }

    public void add(String host) {
        Queue<BSTNode<K>> queue = new LinkedList<>();
        BSTNode<K> theNewNode = newNode(host);
        if (root == null) {
            root = theNewNode;
        } else {
            boolean inserted = false;
            BSTNode<K> current;
            queue.add(root);
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
    }

    public boolean addToNode(BSTNode<K> parent, String host) {
        if (parent.leftChild() == null && parent.rightChild() == null) {
            BSTNode<K> node = new BSTNode<>(parent);
            //node.setContent(parent.getContent());
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

    public List<BSTNode<K>> nodes() {
        List<BSTNode<K>> results = new ArrayList<>();
        getNodes(root, results);
        return results;
    }

    private void getNodes(BSTNode<K> root, List<BSTNode<K>> results) {
        if (root == null) {
            return;
        }
        if (root.getContent() != null) {
            results.add(root);
        }
        getNodes(root.leftChild(), results);
        getNodes(root.rightChild(), results);
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

    public BSTNode<K> findByContent(String content) {
        return findByContent(root, content);
    }

    private BSTNode<K> findByContent(BSTNode<K> root, String content) {
        if (root == null) {
            return null;
        }
        if (root.getContent() != null && content.equals(root.getContent())) {
            return root;
        }
        BSTNode<K> node = findByContent(root.leftChild(), content);
        if (node != null) {
            return node;
        }
        node = findByContent(root.rightChild(), content);
        if (node != null) {
            return node;
        }
        return null;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BST)) return false;

        BST bst = (BST) o;

        if (root != null ? !root.equals(bst.root) : bst.root != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return root != null ? root.hashCode() : 0;
    }
}