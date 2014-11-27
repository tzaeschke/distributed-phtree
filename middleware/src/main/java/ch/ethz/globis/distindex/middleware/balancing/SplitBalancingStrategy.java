package ch.ethz.globis.distindex.middleware.balancing;

import ch.ethz.globis.disindex.codec.io.ClientRequestDispatcher;
import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.middleware.IndexContext;
import ch.ethz.globis.distindex.operation.OpCode;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.request.CommitBalancingRequest;
import ch.ethz.globis.distindex.operation.request.InitBalancingRequest;
import ch.ethz.globis.distindex.operation.request.PutBalancingRequest;
import ch.ethz.globis.distindex.operation.request.Requests;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.pht.PhTreeV;

/**
 * The simple balancing strategy.
 */
public class SplitBalancingStrategy implements BalancingStrategy {

    /** The in-memory index context */
    private IndexContext indexContext;

    /** The request dispatcher */
    private ClientRequestDispatcher<long[], byte[]> requestDispatcher;

    /** The entries that are currently moved to another host. */
    private IndexEntryList<long[], byte[]> buffer;

    public SplitBalancingStrategy(IndexContext indexContext) {
        this.indexContext = indexContext;
    }

    @Override
    public void balance() {
        KeyMapping<long[]> mapping = getMapping();
        String receiverHostId = mapping.getHostForSplitting();
        String currentHostId = indexContext.getHostId();

        IndexEntryList<long[], byte[]> entries = getEntriesForSplitting(currentHostId);
        initBalancing(entries.size(), receiverHostId);
        sendEntries(entries, receiverHostId);
        commitBalancing(receiverHostId);
        updateMapping(currentHostId, receiverHostId);
        removeEntries(entries);
    }

    /**
     * Updates the key mapping after the currentHost zone was split in two and half of it
     * was moved to the receiver host.
     *
     * @param currentHostId                     The hostId of the splitting host.
     * @param receiverHostId                    The hostId of the receiving host.
     */
    private void updateMapping(String currentHostId, String receiverHostId) {
        throw new UnsupportedOperationException("Not yet implemented.");
    }

    /**
     * Remove the re-balanced entries to the new node.
     *
     * @param entries
     */
    private void removeEntries(IndexEntryList<long[], byte[]> entries) {
        PhTreeV<byte[]> tree = indexContext.getTree();
        for (IndexEntry<long[], byte[]> entry : entries) {
            tree.remove(entry.getKey());
        }
    }


    /**
     * Send the entries received as an argument to the host;
     * @param entries
     * @param receivedHostId
     */
    private void sendEntries(IndexEntryList<long[], byte[]> entries, String receivedHostId) {
        PutBalancingRequest<long[]> request;
        Response response;
        for (IndexEntry<long[], byte[]> entry : entries) {
            request = Requests.newPutBalancing(entry.getKey(), entry.getValue());
            response = requestDispatcher.send(receivedHostId, request);
            if (response.getStatus() != OpStatus.SUCCESS) {
                throw new RuntimeException("Receiving host did not accept entry initialization");
            }
        }
    }

    private void initBalancing(int entriesToSend, String receiverHostId) {
        InitBalancingRequest request = Requests.newInitBalancing(entriesToSend);
        Response response = requestDispatcher.send(receiverHostId, request);
        if (response.getStatus() != OpStatus.SUCCESS) {
            throw new RuntimeException("Receiving host did not accept balancing initialization");
        }
    }

    private void commitBalancing( String receiverHostId) {
        CommitBalancingRequest request = Requests.newCommitBalancing();
        Response response = requestDispatcher.send(receiverHostId, request);
        if (response.getStatus() != OpStatus.SUCCESS) {
            throw new RuntimeException("Receiving host did not accept balancing commit");
        }
    }

    private IndexEntryList<long[], byte[]> getEntriesForSplitting(String currentHostId) {

        throw new UnsupportedOperationException("Not currently implemented.");
    }

    private KeyMapping<long[]> getMapping() {
        return indexContext.getClusterService().getMapping();
    }
}
