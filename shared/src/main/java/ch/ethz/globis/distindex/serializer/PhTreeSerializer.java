package ch.ethz.globis.distindex.serializer;


import ch.ethz.globis.pht.PhTree;

import java.io.FileNotFoundException;

public interface PhTreeSerializer {

    public <T> void export(PhTree<T> tree, String filename) throws FileNotFoundException;

    public <T> PhTree<T> load(String filename) throws FileNotFoundException;

}