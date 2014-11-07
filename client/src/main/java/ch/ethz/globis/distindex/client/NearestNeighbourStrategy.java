package ch.ethz.globis.distindex.client;

import ch.ethz.globis.distindex.mapping.KeyMapping;

import java.util.List;

public interface NearestNeighbourStrategy<K> {

    public List<K> getNearestNeighbours(KeyMapping<K> keyMapping,K key, int k);
}
