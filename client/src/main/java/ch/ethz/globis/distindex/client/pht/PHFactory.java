package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.PhTreeF;
import ch.ethz.globis.pht.nv.PhTreeNV;
import ch.ethz.globis.pht.nv.PhTreeNVSolidF;

public interface PHFactory {

    <V> PHTreeIndexProxy<V> createProxy(int dim, int depth);

    <V> PhTree<V> createPHTreeMap(int dim, int depth);

    <V> PhTreeF<V> createPHTreeVD(int dim);

    PhTreeNV createPHTreeSet(int dim, int depth);

    PhTreeNVSolidF createPHTreeRangeSet(int dim, int depth);

    public void close();
}
