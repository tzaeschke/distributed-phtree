package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.middleware.net.BalancingRequestHandler;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.BaseResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.pht.PhTreeV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhTreeBalancingRequestHandler implements BalancingRequestHandler<long[]> {

    private static final Logger LOG = LoggerFactory.getLogger(PhTreeBalancingRequestHandler.class);

    private IndexContext indexContext;
    private IndexEntryList<long[], byte[]> buffer;

    public PhTreeBalancingRequestHandler(IndexContext indexContext) {
        this.indexContext = indexContext;
    }

    @Override
    public Response handleRollback(RollbackBalancingRequest request) {
        PhTreeV<byte[]> tree = indexContext.getTree();
        synchronized (tree) {
            for (IndexEntry<long[], byte[]> entry : buffer) {
                tree.remove(entry.getKey());
            }
        }
        indexContext.endBalancing();
        return ackResponse(request);
    }

    @Override
    public Response handleInit(InitBalancingRequest request) {
        if (indexContext.canStartBalancing()) {
            int size = request.getSize();
            buffer = new IndexEntryList<>(size);

            return ackResponse(request);
        } else {
            return errorResponse(request);
        }
    }

    @Override
    public Response handlePut(PutBalancingRequest<long[]> request) {
        long[] key = request.getKey();
        byte[] value = request.getValue();

        buffer.add(key, value);
        PhTreeV<byte[]> tree = indexContext.getTree();
        synchronized (tree) {
            indexContext.getTree().put(key, value);
        }
        return ackResponse(request);
    }

    @Override
    public Response handleCommit(CommitBalancingRequest request) {
        PhTreeV<byte[]> tree = indexContext.getTree();
        buffer.clear();
        updateBalancingVersion(request);
        String currentHostId = indexContext.getHostId();
        indexContext.getClusterService().setSize(currentHostId, tree.size());
        if (!indexContext.endBalancing()) {
            throw new RuntimeException("Another execution thread is performing balancing in parallel!");
        }
        LOG.info("{} Received commit request.", currentHostId);
        return ackResponse(request);
    }

    private void updateBalancingVersion(CommitBalancingRequest request) {
        String versionString = request.getParameter("balancingVersion");
        int balancingVersion = Integer.parseInt(versionString);
        indexContext.setLastBalancingVersion(balancingVersion);
    }

    private Response ackResponse(Request request) {
        return new BaseResponse(request.getOpCode(), request.getId(), OpStatus.SUCCESS);
    }

    private Response errorResponse(Request request) {
        return new BaseResponse(request.getOpCode(), request.getId(), OpStatus.FAILURE);
    }
}