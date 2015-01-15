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
import ch.ethz.globis.distindex.mapping.zorder.ZMapping;
import ch.ethz.globis.distindex.middleware.IndexContext;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.request.CommitBalancingRequest;
import ch.ethz.globis.distindex.operation.request.InitBalancingRequest;
import ch.ethz.globis.distindex.operation.request.PutBalancingRequest;
import ch.ethz.globis.distindex.operation.request.Requests;
import ch.ethz.globis.distindex.operation.response.BaseResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.pht.PVEntry;
import ch.ethz.globis.pht.PVIterator;
import ch.ethz.globis.pht.PhTreeV;
import org.apache.commons.math.stat.clustering.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ZMappingBalancingStrategy implements BalancingStrategy {

    /** The logger used for this class */
    private static final Logger LOG = LoggerFactory.getLogger(ZMappingBalancingStrategy.class);

    /** The in-memory index context */
    private IndexContext indexContext;

    /** The request dispatcher */
    private RequestDispatcher<long[], byte[]> requestDispatcher;

    private Requests<long[], byte[]> requests;

    private boolean movedToRight = false;

    private int newVersion = 0;

    public ZMappingBalancingStrategy(IndexContext indexContext) {
        this.indexContext = indexContext;
        RequestEncoder requestEncoder = new ByteRequestEncoder<>(new MultiLongEncoderDecoder(), new SerializingEncoderDecoder<>());
        ResponseDecoder<long[], byte[]> responseDecoder = new ByteResponseDecoder<>(new MultiLongEncoderDecoder(), new SerializingEncoderDecoder<byte[]>());
        this.requests = new Requests<>(indexContext.getClusterService());
        this.requestDispatcher = new ClientRequestDispatcher<>(new TCPClient(), requestEncoder, responseDecoder);
    }

    @Override
    public void balance() {
        if (!indexContext.canStartBalancing()) {
            return;
        }
        ClusterService<long[]> cluster = indexContext.getClusterService();
        cluster.lockForWriting();
        cluster.sync();
        KeyMapping<long[]> mapping = cluster.getMapping();

        LOG.info("Host {} started balancing with version {}", indexContext.getHostId(), mapping.getVersion());
        String currentHostId = indexContext.getHostId();
        //printSizes("Initializing balancing operation on host " + currentHostId +". Sizes before balancing");
        String receiverHostId = getHostForSplitting(currentHostId, cluster, mapping);
        if (receiverHostId != null) {
            doBalancing(currentHostId, receiverHostId);
        } else {
            LOG.warn("Failed to find a proper host for balancing.");
        }

        cluster.releaseAfterWriting();
        indexContext.endBalancing();
        //printSizes("Sizes after balancing");
    }

    private void doBalancing(String currentHostId, String receiverHostId) {
        IndexEntryList<long[], byte[]> entries = getEntriesForSplitting(currentHostId);
        boolean canBalance = initBalancing(entries.size(), receiverHostId);
        if (canBalance) {
            sendEntries(entries, receiverHostId);
            commitBalancing(receiverHostId);

            removeEntries(entries);
            updateMapping(currentHostId, receiverHostId, entries);
        }
    }

    private String getHostForSplitting(String currentHostId, ClusterService<long[]> cluster, KeyMapping<long[]> mapping) {
        if (mapping == null) {
            LOG.warn("Mapping is currently not initialized for {}, cannot proceed with balancing.", currentHostId);
            return null;
        }
        String prevId = mapping.getPrevious(currentHostId);
        String nextId = mapping.getNext(currentHostId);
        if (prevId == null) {
            movedToRight = true;
            return nextId;
        }
        if (nextId == null) {
            movedToRight = false;
            return prevId;
        }
        int sizePrev = cluster.getSize(prevId);
        int sizeNext = cluster.getSize(nextId);
        String resultId = (sizePrev < sizeNext) ? prevId : nextId;
        movedToRight = resultId.equals(nextId);
        return resultId;
    }

    private void printSizes(String message) {
        System.out.println(message);
        ClusterService<long[]> cluster = indexContext.getClusterService();
        List<String> hosts = cluster.getOnlineHosts();
        for (String host : hosts) {
            System.out.println(host + ": " + cluster.getSize(host));
        }
    }

    /**
     * Updates the key mapping after the currentHost zone was split in two and half of it
     * was moved to the receiver host.
     *
     * @param currentHostId                     The hostId of the splitting host.
     */
    private void updateMapping(String currentHostId, String receiverHostId, IndexEntryList<long[], byte[]> entries) {
        int entriesMoved = entries.size();
        ClusterService<long[]> cluster = indexContext.getClusterService();
        KeyMapping<long[]> mapping = getMapping();

        cluster.setSize(currentHostId, indexContext.getTree().size());

        ZMapping zmap = (ZMapping) mapping;
        int depth = indexContext.getTree().getDEPTH();
        long[] key;
        if (entriesMoved != 0) {
            if (movedToRight) {
                LOG.info("Balancing {} entries to the right interval.", entriesMoved);
                key = entries.get(0).getKey();
                zmap.changeIntervalEnd(currentHostId, MultidimUtil.previous(key, depth));
            } else {
                LOG.info("Balancing {} entries to the left interval.", entriesMoved);
                key = entries.get(entriesMoved - 1).getKey();
                zmap.changeIntervalEnd(receiverHostId, key);
            }
            zmap.updateTree();
        }
        zmap.setVersion(newVersion);

        LOG.info("Writing mapping with version {} on balancing commit.", mapping.getVersion());
        indexContext.getClusterService().writeCurrentMapping();
    }

    /**
     * Remove the re-balanced entries to the new node.
     *
     * @param entries
     */
    private void removeEntries(IndexEntryList<long[], byte[]> entries) {
        PhTreeV<byte[]> tree = indexContext.getTree();
        synchronized (tree) {
            for (IndexEntry<long[], byte[]> entry : entries) {
                tree.remove(entry.getKey());
            }
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
            request = requests.newPutBalancing(entry.getKey(), entry.getValue());
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
    private boolean initBalancing(int entriesToSend, String receiverHostId) {
        InitBalancingRequest request = requests.newInitBalancing(entriesToSend);
        Response response = requestDispatcher.send(receiverHostId, request, BaseResponse.class);
        if (response.getStatus() != OpStatus.SUCCESS) {
            LOG.error("Receiving host {} did not accept balancing initialization.", receiverHostId);
            return false;
        }
        return true;
    }

    /**
     * Send a commit balancing request to the hose whose hostId was received as an argument.
     *
     * @param receiverHostId
     */
    private void commitBalancing( String receiverHostId) {
        newVersion = getMapping().getVersion() + 1;
        indexContext.setLastBalancingVersion(newVersion);

        CommitBalancingRequest request = requests.newCommitBalancing();
        request.addParamater("balancingVersion", newVersion);

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
        PhTreeV<byte[]> phTree = indexContext.getTree();
        int treeSize = phTree.size();
        int entriesToMove = treeSize / 2;

        IndexEntryList<long[], byte[]> entries = new IndexEntryList<>();
        synchronized (phTree) {
            PVIterator<byte[]> it = phTree.queryExtent();
            if (!movedToRight) {
                for (int i = 0; i < entriesToMove; i++) {
                    PVEntry<byte[]> e = it.nextEntry();
                    entries.add(e.getKey(), e.getValue());
                }
                while (it.hasNext()) {
                    it.next();
                }
            } else {
                for (int i = 0; i < treeSize - entriesToMove; i++) {
                    if (it.hasNext()) {
                        it.next();
                    }
                }
                while (it.hasNext()) {
                    PVEntry<byte[]> e = it.nextEntry();
                    entries.add(e.getKey(), e.getValue());
                }
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
