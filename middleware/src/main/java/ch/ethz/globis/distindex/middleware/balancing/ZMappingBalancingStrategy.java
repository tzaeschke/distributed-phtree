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
import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.BaseResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.pht.PVEntry;
import ch.ethz.globis.pht.PVIterator;
import ch.ethz.globis.pht.PhTreeV;
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
        String receiverHostId = null;
        try {
            ClusterService<long[]> cluster = indexContext.getClusterService();
            KeyMapping<long[]> mapping = cluster.getMapping();

            String currentHostId = indexContext.getHostId();
            BalancingInfo info = getHostForSplitting(currentHostId, cluster, mapping);

            receiverHostId = info.getReceiverHostId();
            if (receiverHostId != null) {
                doBalancing(info);
            } else {
                LOG.warn("Failed to find a proper host for balancing.");
            }
        } catch (Exception e) {
            LOG.error("Exception encountered during re-balancing.", e);
            if (receiverHostId != null) {
                rollbackBalancing(receiverHostId);
            }
        } finally {
            indexContext.endBalancing();
        }
    }

    @Override
    public void balanceAndRemove() {
        //wait until I can balance
        while (!indexContext.canStartBalancing());

        if (tryBalanceToFreeHost()) {
            return;
        }
        ClusterService<long[]> cluster = indexContext.getClusterService();
        KeyMapping<long[]> mapping = cluster.getMapping();
        String currentHostId = indexContext.getHostId();
        String rightHostId = mapping.getNext(currentHostId);
        String leftHostId = mapping.getPrevious(currentHostId);
        balanceToNeighbours(currentHostId, rightHostId, leftHostId);

        indexContext.endBalancing();
    }

    /**
     * Attempt to balance the entries of the current machine to a free node.
     * @return
     */
    private boolean tryBalanceToFreeHost() {
        ClusterService<long[]> cluster = indexContext.getClusterService();
        String currentHostId = indexContext.getHostId();
        String receiverHostId = cluster.getNextFreeHost();

        if (receiverHostId != null) {
            BalancingInfo info = new BalancingInfo();
            info.setInitiatorHostId(currentHostId);
            info.setReceiverHostId(receiverHostId);
            info.setRemoveHost(true);
            info.setReceiverFreeHost(true);
            doBalancingAll(info);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Balance all of the entries in the current hostId to the neighbour hosts.
     *
     * @param currentHostId                 The hostId of the splitting host.
     * @param rightHostId                   The hostId of the right neighbour.
     * @param leftHostId                    The hostId of the left neighbour.
     */
    private void balanceToNeighbours(String currentHostId, String rightHostId, String leftHostId) {
        BalancingInfo info = new BalancingInfo();
        info.setInitiatorHostId(currentHostId);
        info.setReceiverFreeHost(false);

        if (leftHostId == null) {
            info.setMoveToRight(true);
            info.setReceiverHostId(rightHostId);
            info.setRemoveHost(true);
            doBalancingAll(info);
            return;
        }
        if (rightHostId == null) {
            info.setMoveToRight(false);
            info.setReceiverHostId(leftHostId);
            info.setRemoveHost(true);
            doBalancingAll(info);
            return;
        }
        info.setMoveToRight(true);
        info.setReceiverHostId(rightHostId);
        info.setRemoveHost(false);

        doBalancing(info);
        info.setMoveToRight(false);
        info.setReceiverHostId(leftHostId);
        info.setRemoveHost(true);

        doBalancingAll(info);
    }

    /**
     * Perform the balancing with currentHostId as the id of the initiator host and
     * receiverHostId as the id of the receiver host.
     *
     */
    private void doBalancing(BalancingInfo info) {
        LOG.info("Host {} attempts balancing to host {} with mapping version " +
                        indexContext.getClusterService().getMapping().getVersion(),
                        indexContext.getHostId(), info.getReceiverHostId());
        IndexEntryList<long[], byte[]> entries = getEntriesForSplitting(info.isMoveToRight());
        moveEntries(entries, info);
    }

    private void doBalancingAll(BalancingInfo info) {
        IndexEntryList<long[], byte[]> entries = getAllEntries();
        moveEntries(entries, info);
    }

    /**
     * Move the entries received as an argument from the current host to the host with the id
     * stored in the variable receiverHostId.
     *
     * @param entries
     * @param info                              The balancing information linked to this host.
     */
    private void moveEntries(IndexEntryList<long[], byte[]> entries, BalancingInfo info) {
        String receiverHostId = info.getReceiverHostId();

        boolean canBalance = initBalancing(entries.size(), receiverHostId);
        if (canBalance) {
            sendEntries(entries, receiverHostId);
            updateMapping(info, entries);

            commitBalancing(info);
            removeEntries(entries);
        }
    }

    private BalancingInfo getHostForSplitting(String currentHostId, ClusterService<long[]> cluster, KeyMapping<long[]> mapping) {
        BalancingInfo info = new BalancingInfo();

        info.setInitiatorHostId(currentHostId);
        String resultId = cluster.getNextFreeHost();
        if (resultId != null) {
            info.setMoveToRight(true);
            info.setReceiverFreeHost(true);
            info.setReceiverHostId(resultId);
            return info;
        }

        if (mapping == null) {
            LOG.warn("Mapping is currently not initialized for {}, cannot proceed with balancing.", currentHostId);
            return null;
        }
        String prevId = mapping.getPrevious(currentHostId);
        String nextId = mapping.getNext(currentHostId);

        if (prevId == null) {
            info.setReceiverHostId(nextId);
            info.setReceiverFreeHost(false);
            info.setMoveToRight(true);
            return info;
        }
        if (nextId == null) {
            info.setReceiverHostId(prevId);
            info.setReceiverFreeHost(false);
            info.setMoveToRight(false);
            return info;
        }

        int sizePrev = cluster.getSize(prevId);
        int sizeNext = cluster.getSize(nextId);
        resultId = (sizePrev < sizeNext) ? prevId : nextId;
        info.setReceiverFreeHost(false);
        info.setMoveToRight(resultId.equals(nextId));
        info.setReceiverHostId(resultId);
        return info;
    }

    private void printSizes(String message) {
        System.out.println(message);
        ClusterService<long[]> cluster = indexContext.getClusterService();
        List<String> hosts = cluster.getMapping().get();
        for (String host : hosts) {
            System.out.println(host + ": " + cluster.getSize(host));
        }
    }

    /**
     * Updates the key mapping after the currentHost zone was split in two and half of it
     * was moved to the receiver host.
     *
     */
    private void updateMapping(BalancingInfo info, IndexEntryList<long[], byte[]> entries) {
        int entriesMoved = entries.size();
        String currentHostId = info.getInitiatorHostId();
        String receiverHostId = info.getReceiverHostId();
        String freeTargetHost = info.isReceiverFreeHost() ? receiverHostId : null;

        int newVersion;
        boolean movedToRight = info.isMoveToRight();
        ClusterService<long[]> cluster = indexContext.getClusterService();
        KeyMapping<long[]> mapping = getMapping();

        cluster.setSize(currentHostId, indexContext.getTree().size());

        ZMapping zmap = (ZMapping) mapping;
        int depth = indexContext.getTree().getDEPTH();
        long[] key;
        String host;
        if (entriesMoved != 0) {
            //ToDo need to perform the changes to the mapping atomically, i.e replacing a host, etc need to make transactions for that
            if (movedToRight) {
                LOG.info("{} is balancing {} entries to the right interval.", currentHostId, entriesMoved);
                key = MultidimUtil.previous(entries.get(0).getKey(), depth);
                host = currentHostId;
            } else {
                LOG.info("{} is balancing {} entries to the left interval.", currentHostId, entriesMoved);
                key = entries.get(entriesMoved - 1).getKey();
                host = receiverHostId;
            }
            newVersion = cluster.setIntervalEnd(host, key, freeTargetHost);
            LOG.info("{} is writing mapping with version {} on balancing commit.", currentHostId, newVersion);
            if (info.isRemoveHost()) {
                cluster.deregisterHost(info.getInitiatorHostId());
            }
            zmap.updateTree();
            zmap.setVersion(newVersion);
            info.setNewVersion(newVersion);
        }
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
     * @param receiverHostId
     */
    private void sendEntries(IndexEntryList<long[], byte[]> entries, String receiverHostId) {
        String currentHostId = indexContext.getHostId();
        PutBalancingRequest<long[]> request;
        Response response;
        for (IndexEntry<long[], byte[]> entry : entries) {
            request = requests.newPutBalancing(entry.getKey(), entry.getValue());
            response = requestDispatcher.send(receiverHostId, request, BaseResponse.class);
            if (response.getStatus() != OpStatus.SUCCESS) {
                String message = String.format("[%s] Receiving host %s did not accept entry during balancing", currentHostId, receiverHostId);
                throw new RuntimeException(message);
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
        String currentHostId = indexContext.getHostId();
        PhTreeV<byte[]> tree = indexContext.getTree();
        InitBalancingRequest request = requests.newInitBalancing(entriesToSend, tree.getDIM(), tree.getDEPTH());
        Response response = requestDispatcher.send(receiverHostId, request, BaseResponse.class);
        if (response.getStatus() != OpStatus.SUCCESS) {
            LOG.error("[{}] Receiving host {} did not accept balancing initialization.", currentHostId, receiverHostId);
            return false;
        }
        return true;
    }

    /**
     * Send a commit balancing request to the hose whose hostId was received as an argument.
     *
     */
    private void commitBalancing(BalancingInfo info) {
        String receiverHostId = info.getReceiverHostId();
        int newVersion = info.getNewVersion();
        indexContext.setLastBalancingVersion(newVersion);
        String currentHostId = indexContext.getHostId();
        CommitBalancingRequest request = requests.newCommitBalancing();
        request.addParamater("balancingVersion", newVersion);

        Response response = requestDispatcher.send(receiverHostId, request, BaseResponse.class);
        if (response.getStatus() != OpStatus.SUCCESS) {
            String message = String.format("[%s] Receiving host %s did not accept balancing commit", currentHostId, receiverHostId);
            throw new RuntimeException(message);
        }
    }

    /**
     * Send a rollback balancing request.
     * @param receiverHostId
     */
    private void rollbackBalancing(String receiverHostId) {
        String currentHostId = indexContext.getHostId();
        RollbackBalancingRequest request = requests.newRollbackBalancing();
        Response response = requestDispatcher.send(receiverHostId, request, BaseResponse.class);
        if (response.getStatus() != OpStatus.SUCCESS) {
            LOG.error("[{}] Receiving host {} did not accept balancing rollback.", currentHostId, receiverHostId);
        }
    }

    /**
     * Get a list of entries that will be sent to the other host to perform the re-balancing.
     * @return
     */
    private IndexEntryList<long[], byte[]> getEntriesForSplitting(boolean movedToRight) {
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

    private IndexEntryList<long[], byte[]> getAllEntries() {
        PhTreeV<byte[]> phTree = indexContext.getTree();

        IndexEntryList<long[], byte[]> entries = new IndexEntryList<>();
        synchronized (phTree) {
            PVIterator<byte[]> it = phTree.queryExtent();
            PVEntry<byte[]> e = it.nextEntry();
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

    /**
     * Contains the information necessary to a good split.
     */
    class BalancingInfo {

        private String receiverHostId;
        private String initiatorHostId;
        private boolean receiverFreeHost = false;
        private boolean moveToRight = false;
        private boolean removeHost = false;

        private int newVersion = 0;

        public String getReceiverHostId() {
            return receiverHostId;
        }

        public void setReceiverHostId(String receiverHostId) {
            this.receiverHostId = receiverHostId;
        }

        public String getInitiatorHostId() {
            return initiatorHostId;
        }

        public void setInitiatorHostId(String initiatorHostId) {
            this.initiatorHostId = initiatorHostId;
        }

        public boolean isMoveToRight() {
            return moveToRight;
        }

        public void setMoveToRight(boolean moveToRight) {
            this.moveToRight = moveToRight;
        }

        public int getNewVersion() {
            return newVersion;
        }

        public void setNewVersion(int newVersion) {
            this.newVersion = newVersion;
        }

        public boolean isReceiverFreeHost() {
            return receiverFreeHost;
        }

        public void setReceiverFreeHost(boolean receiverFreeHost) {
            this.receiverFreeHost = receiverFreeHost;
        }

        public void setRemoveHost(boolean removeHost) {
            this.removeHost = removeHost;
        }

        public boolean isRemoveHost() {
            return removeHost;
        }
    }
}
