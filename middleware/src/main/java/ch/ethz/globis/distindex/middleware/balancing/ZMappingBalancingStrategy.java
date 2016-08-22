/*
This file is part of PH-Tree:
A multi-dimensional indexing and storage structure.

Copyright (C) 2011-2015
Eidgenössische Technische Hochschule Zürich (ETH Zurich)
Institute for Information Systems
GlobIS Group
Bogdan Vancea, Tilmann Zaeschke
zaeschke@inf.ethz.ch or zoodb@gmx.de

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
package ch.ethz.globis.distindex.middleware.balancing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import ch.ethz.globis.distindex.operation.request.RollbackBalancingRequest;
import ch.ethz.globis.distindex.operation.response.BaseResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;
import ch.ethz.globis.distindex.util.MultidimUtil;
import ch.ethz.globis.phtree.PhEntry;
import ch.ethz.globis.phtree.PhTree;
import ch.ethz.globis.phtree.PhTree.PhIterator;

public class ZMappingBalancingStrategy implements BalancingStrategy {

    /** The logger used for this class */
    private static final Logger LOG = LoggerFactory.getLogger(ZMappingBalancingStrategy.class);

    /** The in-memory index context */
    private IndexContext indexContext;

    /** The request dispatcher */
    private RequestDispatcher requestDispatcher;

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

        String currentHostId = indexContext.getHostId();
        String freeHostId = getClusterService().getNextFreeHost();
        if (freeHostId != null) {
            doBalancingFreeAndRemove(currentHostId, freeHostId);
        }
        ClusterService<long[]> cluster = getClusterService();
        KeyMapping<long[]> mapping = cluster.getMapping();

        String rightHostId = mapping.getNext(currentHostId);
        String leftHostId = mapping.getPrevious(currentHostId);
        balanceToNeighbours(currentHostId, rightHostId, leftHostId);

        indexContext.endBalancing();
    }

    /**
     * Balance all of the entries in the current hostId to the neighbour hosts.
     *
     * @param currentHostId                 The hostId of the splitting host.
     * @param rightHostId                   The hostId of the right neighbour.
     * @param leftHostId                    The hostId of the left neighbour.
     */
    private void balanceToNeighbours(String currentHostId, String rightHostId, String leftHostId) {
        if (leftHostId == null) {
            doBalancingRightAndRemove(currentHostId, rightHostId);
            return;
        }
        if (rightHostId == null) {
            doBalancingLeftAndRemove(currentHostId, leftHostId);
            return;
        }
        doBalancingBoth(currentHostId, rightHostId, leftHostId);

    }

    private void doBalancingFreeAndRemove(String currentHostId, String freeHostId) {
        LOG.info("Removing host {} and moving all content to free host {}", currentHostId, freeHostId);
        int totalEntries = indexContext.getTree().size();
        boolean canBalance = initBalancing(totalEntries, freeHostId);
        if (!canBalance) {
            throw new UnsupportedOperationException("Cannot balance to the right neighbour.");
        }
        IndexEntryList<long[], byte[]> entries = getAllEntries();
        sendEntries(entries, freeHostId);
        int newVersion = updateMappingRemoveToFree(currentHostId, freeHostId);
        commitBalancing(currentHostId, freeHostId, newVersion);
        removeEntries(entries);
    }

    private int updateMappingRemoveToFree(String currentHostId, String freeHostId) {
        getClusterService().mergeWithRightFree(currentHostId, freeHostId);
        return getMapping().getVersion();
    }

    private void doBalancingLeftAndRemove(String currentHostId, String leftHostId) {
        LOG.info("Removing host {} and moving all content to left host {}", currentHostId, leftHostId);
        int totalEntries = indexContext.getTree().size();
        boolean canBalance = initBalancing(totalEntries, leftHostId);
        if (!canBalance) {
            throw new UnsupportedOperationException("Cannot balance to the left neighbour.");
        }
        IndexEntryList<long[], byte[]> entries = getAllEntries();
        sendEntries(entries, leftHostId);
        int newVersion = updateMappingRemoveToLeft(currentHostId, leftHostId);
        commitBalancing(currentHostId, leftHostId, newVersion);
        removeEntries(entries);
    }

    private int updateMappingRemoveToLeft(String currentHostId, String leftHostId) {
        getClusterService().mergeWithLeft(currentHostId, leftHostId);
        return getMapping().getVersion();
    }

    private void doBalancingRightAndRemove(String currentHostId, String rightHostId) {
        LOG.info("Removing host {} and moving all content to right host {}", currentHostId, rightHostId);
        int totalEntries = indexContext.getTree().size();
        boolean canBalance = initBalancing(totalEntries, rightHostId);
        if (!canBalance) {
            throw new UnsupportedOperationException("Cannot balance to the right neighbour.");
        }
        IndexEntryList<long[], byte[]> entries = getAllEntries();
        sendEntries(entries, rightHostId);
        int newVersion = updateMappingRemoveToRight(currentHostId);
        commitBalancing(currentHostId, rightHostId, newVersion);
        removeEntries(entries);
    }

    private int updateMappingRemoveToRight(String currentHostId) {
        getClusterService().mergeWithRight(currentHostId);
        return getMapping().getVersion();
    }

    private void doBalancingBoth(String currentHostId, String rightHostId, String leftHostId) {
        LOG.info("Removing host {} and moving all content to left host {} and right host {} ",
                new Object[] { currentHostId, leftHostId, rightHostId });
        int totalEntries = indexContext.getTree().size();
        int nrEntriesLeft = totalEntries / 2;
        int nrEntriesRight = totalEntries - nrEntriesLeft;

        boolean canBalanceRight = initBalancing(nrEntriesRight, rightHostId);
        boolean canBalanceLeft = initBalancing(nrEntriesLeft, leftHostId);

        if (!canBalanceLeft || !canBalanceRight) {
            //ToDo should at least try to balance to the right one or the left one
            throw new UnsupportedOperationException("Cannot balance to the left and right neighbours.");
        }
        IndexEntryList<long[], byte[]> entriesRight = getEntriesForSplitting(true);
        sendEntries(entriesRight, rightHostId);
        removeEntries(entriesRight);
        IndexEntryList<long[], byte[]> entries = getAllEntries();
        sendEntries(entries, leftHostId);
        int newVersion = updateMapping(currentHostId, leftHostId, entries);
        commitBalancing(currentHostId, rightHostId, newVersion);
        commitBalancing(currentHostId, leftHostId, newVersion);
        removeEntries(entries);
    }

    /**
     * Perform the balancing with currentHostId as the id of the initiator host and
     * receiverHostId as the id of the receiver host.
     *
     */
    private void doBalancing(BalancingInfo info) {
        LOG.info("Host {} attempts balancing to host {} with mapping version " +
                        getClusterService().getMapping().getVersion(),
                        indexContext.getHostId(), info.getReceiverHostId());
        IndexEntryList<long[], byte[]> entries = getEntriesForSplitting(info.isMoveToRight());
        moveEntries(entries, info);
    }

    //TODO remove? TZ
//    private void doBalancingAll(BalancingInfo info) {
//        IndexEntryList<long[], byte[]> entries = getAllEntries();
//        moveEntries(entries, info);
//    }

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

    //TODO remove? TZ
//    private void printSizes(String message) {
//        System.out.println(message);
//        ClusterService<long[]> cluster = getClusterService();
//        List<String> hosts = cluster.getMapping().get();
//        for (String host : hosts) {
//            System.out.println(host + ": " + cluster.getSize(host));
//        }
//    }

    private int updateMapping(String currentHostId, String leftHostId, IndexEntryList<long[], byte[]> entries) {
        long[] newKeyForLeft = entries.get(entries.size() - 1).getKey();
        ZKClusterService cluster = getClusterService();
        cluster.setIntervalEndAndDelete(leftHostId, newKeyForLeft, currentHostId);
        return cluster.getMapping().getVersion();
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
        ZKClusterService cluster = getClusterService();
        KeyMapping<long[]> mapping = getMapping();

        cluster.setSize(currentHostId, indexContext.getTree().size());

        ZMapping zmap = (ZMapping) mapping;
        int depth = indexContext.getTree().getBitDepth();
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
        PhTree<byte[]> tree = indexContext.getTree();

        for (IndexEntry<long[], byte[]> entry : entries) {
            tree.remove(entry.getKey());
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
        PhTree<byte[]> tree = indexContext.getTree();
        InitBalancingRequest request = requests.newInitBalancing(entriesToSend, tree.getDim(), tree.getBitDepth());
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
        String currentHostId = info.getInitiatorHostId();
        int newVersion = info.getNewVersion();
        commitBalancing(currentHostId, receiverHostId, newVersion);
    }

    private void commitBalancing(String currentHostId, String receiverHostId, int newVersion) {
        indexContext.setLastBalancingVersion(newVersion);

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
        PhTree<byte[]> phTree = indexContext.getTree();
        int treeSize = phTree.size();
        int entriesToMove = treeSize / 2;

        IndexEntryList<long[], byte[]> entries = new IndexEntryList<>();

        PhIterator<byte[]> it = phTree.queryExtent();
        if (!movedToRight) {
            for (int i = 0; i < entriesToMove; i++) {
                PhEntry<byte[]> e = it.nextEntry();
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
                PhEntry<byte[]> e = it.nextEntry();
                entries.add(e.getKey(), e.getValue());
            }
        }

        return entries;
    }

    private IndexEntryList<long[], byte[]> getAllEntries() {
        PhTree<byte[]> phTree = indexContext.getTree();

        IndexEntryList<long[], byte[]> entries = new IndexEntryList<>();

        PhIterator<byte[]> it = phTree.queryExtent();
        while (it.hasNext()) {
            PhEntry<byte[]> e = it.nextEntry();
            entries.add(e.getKey(), e.getValue());
        }

        return entries;
    }

    /**
     * @return                                  The current key-host mapping.
     */
    private ZMapping getMapping() {
        return (ZMapping) getClusterService().getMapping();
    }

    private ZKClusterService getClusterService() {
        return (ZKClusterService) indexContext.getClusterService();
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
