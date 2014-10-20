package ch.ethz.globis.distindex.mapping.bst;

public class BSTNode<K> {

    private BSTNode<K> left, right;
    private String content;
    private K key;
    private int size;

    public BSTNode<K> getLeft() {
        return left;
    }

    public void setLeft(BSTNode<K> left) {
        this.left = left;
    }

    public BSTNode<K> getRight() {
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
