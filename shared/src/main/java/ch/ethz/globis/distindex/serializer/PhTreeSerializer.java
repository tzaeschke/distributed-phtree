package ch.ethz.globis.distindex.serializer;


import ch.ethz.globis.pht.PhTreeV;

import java.io.FileNotFoundException;

public interface PhTreeSerializer {

    public <T> void export(PhTreeV<T> tree, String filename) throws FileNotFoundException;

    public <T> PhTreeV<T> load(String filename) throws FileNotFoundException;

}