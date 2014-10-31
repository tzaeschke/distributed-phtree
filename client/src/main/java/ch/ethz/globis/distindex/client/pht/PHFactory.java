package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.distindex.client.util.UnsafeUtil;
import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.PhTreeRangeD;
import ch.ethz.globis.pht.PhTreeV;
import ch.ethz.globis.pht.PhTreeVProxy;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class PHFactory {

    private String zkHost;
    private int zkPort;

    public PHFactory(String zkHost, int zkPort) {
        this.zkHost = zkHost;
        this.zkPort = zkPort;
    }

    public <V> PhTreeV<V> createPHTreeMap(int dim, int depth, Class<V> valueClass) {
        DistributedPHTreeProxy<V> proxy = new DistributedPHTreeProxy<>(zkHost, zkPort, valueClass);
        proxy.create(dim, depth);
        return new DistributedPhTreeV<>(proxy);
    }

    public PhTree createPHTreeSet(int dim, int depth) {
        PhTreeV<Object> backingTree = createPHTreeMap(dim, depth, Object.class);

        PhTree tree = PhTree.create(dim, depth);
        Unsafe unsafe = UnsafeUtil.get();
        try {
            Field field = PhTreeVProxy.class.getDeclaredField("tree");
            long offset = unsafe.objectFieldOffset(field);
            unsafe.putObject(tree, offset, backingTree);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        return tree;
    }

    public PhTreeRangeD createPHTreeRangeSet(int dim, int depth) {
        PhTree backingTree = createPHTreeSet(2 * dim, depth);

        PhTreeRangeD tree = new PhTreeRangeD(dim);
        Unsafe unsafe = UnsafeUtil.get();
        try {
            Field field = PhTreeRangeD.class.getDeclaredField("pht");
            long offset = unsafe.objectFieldOffset(field);
            unsafe.putObject(tree, offset, backingTree);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        return tree;
    }
}