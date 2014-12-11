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
import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.middleware.IndexContext;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.request.CommitBalancingRequest;
import ch.ethz.globis.distindex.operation.request.InitBalancingRequest;
import ch.ethz.globis.distindex.operation.request.PutBalancingRequest;
import ch.ethz.globis.distindex.operation.request.Requests;
import ch.ethz.globis.distindex.operation.response.BaseResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.pht.PVEntry;
import ch.ethz.globis.pht.PVIterator;
import ch.ethz.globis.pht.PhTreeV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ZMappingBalancing implements BalancingStrategy {

    /** The logger used for this class */
    private static final Logger LOG = LoggerFactory.getLogger(ZMappingBalancing.class);

    /** The in-memory index context */
    private IndexContext indexContext;

    /** The request dispatcher */
    private RequestDispatcher<long[], byte[]> requestDispatcher;

    public ZMappingBalancing(IndexContext indexContext) {
        this.indexContext = indexContext;
        RequestEncoder requestEncoder = new ByteRequestEncoder<>(new MultiLongEncoderDecoder(), new SerializingEncoderDecoder<>());
        ResponseDecoder<long[], byte[]> responseDecoder = new ByteResponseDecoder<>(new MultiLongEncoderDecoder(), new SerializingEncoderDecoder<byte[]>());

        this.requestDispatcher = new ClientRequestDispatcher<>(new TCPClient(), requestEncoder, responseDecoder);
    }

    @Override
    public void balance() {
        List<String> hosts = indexContext.getClusterService().getOnlineHosts();
        KeyMapping<long[]> mapping = indexContext.getClusterService().getMapping();

        System.out.println("Sizes");
        for (String host : hosts) {
            System.out.println(host + ": " + mapping.getSize(host));
        }

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

        removeEntries(entries);
        updateMapping(currentHostId);
    }

    /**
     * Updates the key mapping after the currentHost zone was split in two and half of it
     * was moved to the receiver host.
     *
     * @param currentHostId                     The hostId of the splitting host.
     */
    private void updateMapping(String currentHostId) {
        getMapping().setSize(currentHostId, indexContext.getTree().size());
        indexContext.getClusterService().writeCurrentMapping();
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
        KeyMapping<long[]> mapping = getMapping();
        String receiver = getMapping().getHostForSplitting(currentHostId);

        int entriesToMove = (mapping.getSize(currentHostId) - mapping.getSize(receiver)) / 2;
        IndexEntryList<long[], byte[]> entries = new IndexEntryList<>();
        if (mapping.getNext(receiver).equals(currentHostId)) {
            //move to left
            PVIterator<byte[]> it = indexContext.getTree().queryExtent();
            for (int i = 0; i < entriesToMove; i++) {
                PVEntry<byte[]> e = it.nextEntry();
                entries.add(e.getKey(), e.getValue());
            }
            while (it.hasNext()) {
                it.next();
            }
        } else {
            //move to right
            PVIterator<byte[]> it = indexContext.getTree().queryExtent();
            for (int i = 0; i < mapping.getSize(currentHostId) - entriesToMove; i++) {
                it.next();
            }
            while (it.hasNext()) {
                PVEntry<byte[]> e = it.nextEntry();
                entries.add(e.getKey(), e.getValue());
            }
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
