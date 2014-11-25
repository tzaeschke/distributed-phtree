package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.pht.PhTreeV;

/**
 * The in-memory context associated with the index.
 */
public class IndexContext {

    private PhTreeV<byte[]> tree;

    public void setTree(PhTreeV<byte[]> tree) {
        this.tree = tree;
    }

    public PhTreeV<byte[]> getTree() {
        return tree;
    }
}