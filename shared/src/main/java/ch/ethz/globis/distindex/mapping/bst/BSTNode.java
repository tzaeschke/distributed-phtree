package ch.ethz.globis.distindex.mapping.bst;

public class BSTNode<K> {

    private BSTNode<K> left, right;
    private String content;
    private K key;
    private int size;

    public BSTNode() {
    }

    public BSTNode(String content, int size) {
        this.content = content;
    }

    public BSTNode(BSTNode<K> original) {
        this(original.getContent(), original.getSize());
    }

    public void clear() {
        this.setKey(null);
        this.setSize(0);
    }

    public BSTNode<K> leftChild() {
        return left;
    }

    public void setLeft(BSTNode<K> left) {
        this.left = left;
    }

    public BSTNode<K> rightChild() {
        return right;
    }

    public void setRight(BSTNode<K> right) {
        this.right = right;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
