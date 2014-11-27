package ch.ethz.globis.distindex.mapping.bst;

public class BSTNode {

    private BSTNode left, right;
    private String content;
    private int size;
    private String prefix;

    public BSTNode() {
    }

    public BSTNode(String prefix, String content, int size) {
        this.prefix = prefix;
        this.content = content;
        this.size = size;
    }

    public BSTNode(BSTNode original) {
        this.content = original.getContent();
        this.size = original.getSize();
    }

    public void clear() {
        this.setContent(null);
        this.setSize(0);
    }

    public BSTNode leftChild() {
        return left;
    }

    public void setLeft(BSTNode left) {
        this.left = left;
    }

    public BSTNode rightChild() {
        return right;
    }

    public void setRight(BSTNode right) {
        this.right = right;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BSTNode)) return false;

        BSTNode node = (BSTNode) o;

        if (size != node.size) return false;
        if (content != null ? !content.equals(node.content) : node.content != null) return false;
        if (prefix != null ? !prefix.equals(node.prefix) : node.prefix != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = content != null ? content.hashCode() : 0;
        result = 31 * result + size;
        result = 31 * result + (prefix != null ? prefix.hashCode() : 0);
        return result;
    }
}
