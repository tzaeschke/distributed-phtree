package ch.ethz.globis.distindex.client;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.api.IndexIterator;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.operation.response.ResultResponse;

import java.io.Closeable;
import java.io.IOException;

/**
 * Implements an iterator for entries over a distributed index.
 *
 * @param <K>                                   The class of the keys.
 * @param <V>                                   The class of the values.
 */
public class DistIndexIterator<K, V> implements IndexIterator<K, V>, Closeable, AutoCloseable {

    /** The index over which the iterator is running. */
    IndexProxy<K, V> indexProxy;

    /** The entries that are currently buffered*/
    IndexEntryList<K, V> entryBuffer;

    /** The number of entries to bring in a batch request */
    private int batchSize = 2;

    /** The id of the host from which batches are currently brought. */
    private String currentHostId;

    /** The mapping between the keys and the hosts */
    private KeyMapping<K> keyMapping;

    /** The id of the iterator */
    private String iteratorId = "";

    /** The start of the iterator range */
    private K start;

    /** The end of the iterator range */
    private K end;

    /** Is true if the iterator is ranged (start and end are not null), false otherwise*/
    private boolean isRanged;

    /** the position within the entry buffer */
    private int position = -1;

    /**
     * Constructor for a non-ranged iterator.
     *
     * @param indexProxy                            The index proxy instance which created the iterator
     * @param keyMapping                            The mapping between the keys and the index hosts.
     */
    public DistIndexIterator(IndexProxy<K, V> indexProxy, KeyMapping<K> keyMapping) {
        this.indexProxy = indexProxy;
        this.keyMapping = keyMapping;
        this.currentHostId = keyMapping.getFirst();
        this.isRanged = false;
        getRemoteEntries();
    }

    /**
     * Constructor for a range iterator.
     *
     * @param indexProxy                            The index proxy instance which created the iterator
     * @param keyMapping                            The mapping between the keys and the index hosts.
     * @param start                                 The start key for the iterator.
     * @param end                                   The end key for the iterator.
     */
    public DistIndexIterator(IndexProxy<K, V> indexProxy, KeyMapping<K> keyMapping, K start, K end) {
        this.indexProxy = indexProxy;
        this.keyMapping = keyMapping;
        this.currentHostId = keyMapping.getFirst();
        this.start = start;
        this.end = end;
        this.isRanged = true;
        getRemoteEntries();
    }

    @Override
    public boolean hasNext() {
        if (position < entryBuffer.size()) {
            return true;
        }
        entryBuffer.clear();
        getRemoteEntries();
        return entryBuffer.size() > 0;
    }

    @Override
    public IndexEntry<K, V> next() {
        if (position == entryBuffer.size()) {
            getRemoteEntries();
        }
        if (position == entryBuffer.size()) {
            return null;
        }
        return entryBuffer.get(position++);
    }

    /**
     * Get a batch of entries from the remote hosts and keep them in the entryBuffer.
     *
     * The iterator cycles through all of the hosts holding data. Once it has retrieved all batches from a host,
     * it will find the next host in the key ordering and will query that one.
     * Once a host sends the last batch of entries, it sets the iteratorId to "" to let the iterator know it has to
     * query a different host for the next batch.
     */
    private void getRemoteEntries() {

        //this means all of the entries for all of the hosts have been queried
        if (currentHostId == null) {
            return;
        }

        ResultResponse<K, V> response;
        if (isRanged) {
            response = indexProxy.getNextBatch(currentHostId, iteratorId, batchSize, start, end);
        } else {
            response = indexProxy.getNextBatch(currentHostId, iteratorId, batchSize);
        }
        if (response != null) {
            //if the remote index set the iteratorId to "", it has just sent us the last batch and
            //we can change to the next host.
            if (response.getIteratorId().equals("")) {
                currentHostId = keyMapping.getNext(currentHostId);
            }
            iteratorId = response.getIteratorId();
            entryBuffer = response.getEntries();
        } else {
            entryBuffer = new IndexEntryList<>();
        }
        position = 0;
    }

    @Override
    public void close() throws IOException {
        indexProxy.closeIterator(currentHostId, iteratorId);
        entryBuffer = null;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}