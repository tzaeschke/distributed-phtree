package ch.ethz.globis.distindex.middleware.balancing;

import ch.ethz.globis.disindex.codec.ByteRequestEncoder;
import ch.ethz.globis.disindex.codec.ByteResponseDecoder;
import ch.ethz.globis.disindex.codec.api.RequestEncoder;
import ch.ethz.globis.disindex.codec.api.ResponseDecoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.disindex.codec.field.SerializingEncoderDecoder;
import ch.ethz.globis.disindex.codec.io.ClientRequestDispatcher;
import ch.ethz.globis.disindex.codec.io.RequestDispatcher;
import ch.ethz.globis.disindex.codec.io.TCPClient;
import ch.ethz.globis.disindex.codec.util.BitUtils;
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
import ch.ethz.globis.distindex.operation.response.BaseResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.pht.BitTools;
import ch.ethz.globis.pht.PVEntry;
import ch.ethz.globis.pht.PVIterator;
import ch.ethz.globis.pht.PhTreeV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The simple balancing strategy.
 */
public class SplitEvenHalfBalancingStrategy implements BalancingStrategy {

    /** The in-memory index context */
    private IndexContext indexContext;

    /** The request dispatcher */
    private RequestDispatcher<long[], byte[]> requestDispatcher;

    private static final Logger LOG = LoggerFactory.getLogger(SplitEvenHalfBalancingStrategy.class);

    public SplitEvenHalfBalancingStrategy(IndexContext indexContext) {
        this.indexContext = indexContext;
        RequestEncoder requestEncoder = new ByteRequestEncoder<>(new MultiLongEncoderDecoder(), new SerializingEncoderDecoder<>());
        ResponseDecoder<long[], byte[]> responseDecoder = new ByteResponseDecoder<>(new MultiLongEncoderDecoder(), new SerializingEncoderDecoder<byte[]>());

        this.requestDispatcher = new ClientRequestDispatcher<>(new TCPClient(), requestEncoder, responseDecoder);
    }

    /**
     * Perform the balancing operation. This operation has the following steps:
     *
     * 1.   Find the host that will be the receiver of the extra entries.
     * 2.   Perform the split and determine which entries will be moved.
     * 3.   Initialize the balancing operation through an init-balancing request.
     * 4.   Send the entries, one at a time
     * 5.   Send a commit balancing message to the host
     * 6.   Update the mapping server.
     * 7.
     *
     */
    @Override
    public void balance() {
        KeyMapping<long[]> mapping = getMapping();
        String currentHostId = indexContext.getHostId();
        String receiverHostId = mapping.getHostForSplitting(currentHostId);
        if (receiverHostId == null) {
            LOG.error("Failed to find a proper host for balancing");
            return;
        }
        IndexEntryList<long[], byte[]> entries = getEntriesForSplitting(currentHostId);
        initBalancing(entries.size(), receiverHostId);
        sendEntries(entries, receiverHostId);
        commitBalancing(receiverHostId);
        updateMapping(currentHostId, receiverHostId, entries.size());
        removeEntries(entries);
    }

    /**
     * Updates the key mapping after the currentHost zone was split in two and half of it
     * was moved to the receiver host.
     *
     * @param currentHostId                     The hostId of the splitting host.
     * @param receiverHostId                    The hostId of the receiving host.
     */
    private void updateMapping(String currentHostId, String receiverHostId, int newSize) {
        getMapping().split(currentHostId, receiverHostId, newSize);
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
            response = requestDispatcher.send(receivedHostId, request, BaseResponse.class);
            if (response.getStatus() != OpStatus.SUCCESS) {
                throw new RuntimeException("Receiving host did not accept entry initialization");
            }
        }
    }

    /**
     * Send an initialize balancing request to the host whose hostId was received as an argument.
     *
     * @param entriesToSend
     * @param receiverHostId
     */
    private void initBalancing(int entriesToSend, String receiverHostId) {
        InitBalancingRequest request = Requests.newInitBalancing(entriesToSend);
        Response response = requestDispatcher.send(receiverHostId, request, BaseResponse.class);
        if (response.getStatus() != OpStatus.SUCCESS) {
            throw new RuntimeException("Receiving host did not accept balancing initialization");
        }
    }

    /**
     * Send a commit balancing request to the hose whose hostId was received as an argument.
     *
     * @param receiverHostId
     */
    private void commitBalancing( String receiverHostId) {
        CommitBalancingRequest request = Requests.newCommitBalancing();
        Response response = requestDispatcher.send(receiverHostId, request, BaseResponse.class);
        if (response.getStatus() != OpStatus.SUCCESS) {
            throw new RuntimeException("Receiving host did not accept balancing commit");
        }
    }

    /**
     * Get a list of entries that will be sent to the other host to perform the re-balancing.
     *
     * @param currentHostId
     * @return
     */
    private IndexEntryList<long[], byte[]> getEntriesForSplitting(String currentHostId) {
        //get the zone that needs to be split
        PhTreeV<byte[]> treeV = indexContext.getTree();
        int dim = treeV.getDIM();
        int depth = treeV.getDEPTH();
        KeyMapping<long[]> mapping = getMapping();
        String prefix = mapping.getLargestZone(currentHostId);

        //get the iterator over the zone that will be moved
        long[] start = BitUtils.generateRangeStart(prefix, dim, depth);
        long[] end = BitUtils.generateRangeEnd(prefix, dim, depth);
        PVIterator<byte[]> query = treeV.query(start, end);

        //retrieve all entries that will be moved
        IndexEntryList<long[], byte[]> entries = new IndexEntryList<>();
        while (query.hasNext()) {
            PVEntry<byte[]> e = query.nextEntry();
            entries.add(e.getKey(), e.getValue());
        }
        return entries;
    }

    /**
     * @return                                  The current key-host mapping.
     */
    private KeyMapping<long[]> getMapping() {
        return indexContext.getClusterService().getMapping();
    }
}
