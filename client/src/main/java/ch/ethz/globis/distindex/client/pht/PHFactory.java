package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.PhTreeRangeD;
import ch.ethz.globis.pht.PhTreeV;
import ch.ethz.globis.pht.PhTreeVD;

/**
 * Created by bvancea on 13.11.14.
 */
public interface PHFactory {
    <V> PHTreeIndexProxy<V> createProxy(int dim, int depth);

    <V> PhTreeV<V> createPHTreeMap(int dim, int depth);

    <V> PhTreeVD<V> createPHTreeVD(int dim);

    PhTree createPHTreeSet(int dim, int depth);

    PhTreeRangeD createPHTreeRangeSet(int dim, int depth);
}
