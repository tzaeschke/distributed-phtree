package ch.ethz.globis.distindex.client.pht;

import org.jboss.netty.handler.codec.replay.UnreplayableOperationException;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexIterator;
import ch.ethz.globis.pht.PhEntry;
import ch.ethz.globis.pht.PhTree.PhQuery;

/**
 * Main implementation of the PH Tree key-value iterator, backed by a proxy iterator.
 *
 * @param <V>                                   The value class for the iterator.
 */
public class DistributedPhTreeIterator<V> implements PhQuery<V> {

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

	@Override
	public void reset(long[] min, long[] max) {
		// TODO Auto-generated method stub
		throw new UnreplayableOperationException();
	}

	@Override
	public PhEntry<V> nextEntryReuse() {
		// TODO Auto-generated method stub
		return nextEntry();
	}
}
