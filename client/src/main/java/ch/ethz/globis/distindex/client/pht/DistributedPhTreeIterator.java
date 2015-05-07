package ch.ethz.globis.distindex.client.pht;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexIterator;
import ch.ethz.globis.pht.PhEntry;
import ch.ethz.globis.pht.PhTree.PhIterator;

/**
 * Main implementation of the PH Tree key-value iterator, backed by a proxy iterator.
 *
 * @param <V>                                   The value class for the iterator.
 */
public class DistributedPhTreeIterator<V> implements PhIterator<V> {

    private IndexIterator<long[], V> proxyIterator;

    public DistributedPhTreeIterator(IndexIterator<long[], V> proxyIterator) {
        this.proxyIterator = proxyIterator;
    }

    @Override
    public long[] nextKey() {
        return proxyIterator.next().getKey();
    }

    @Override
    public V nextValue() {
        return proxyIterator.next().getValue();
    }

    @Override
    public PhEntry<V> nextEntry() {
        IndexEntry<long[], V> currentEntry = proxyIterator.next();
        return new PhEntry<>(currentEntry.getKey(), currentEntry.getValue());
    }

    @Override
    public boolean hasNext() {
        return proxyIterator.hasNext();
    }

    @Override
    public V next() {
        return proxyIterator.next().getValue();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove operation not currently supported.");
    }
}
