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
package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.middleware.net.BalancingRequestHandler;
import ch.ethz.globis.distindex.operation.OpStatus;
import ch.ethz.globis.distindex.operation.request.*;
import ch.ethz.globis.distindex.operation.response.BaseResponse;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.phtree.PhTree;
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
        PhTree<byte[]> tree = indexContext.getTree();
        for (IndexEntry<long[], byte[]> entry : buffer) {
            tree.remove(entry.getKey());
        }
        indexContext.endBalancing();
        return ackResponse(request);
    }

    @Override
    public Response handleInit(InitBalancingRequest request) {
        if (indexContext.canStartBalancing()) {
            int size = request.getSize();
            buffer = new IndexEntryList<>(size);

            int dim = request.getDim();
            int depth = request.getDepth();
            if (indexContext.getTree() == null) {
                indexContext.initTree(dim, depth);
            }
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
        PhTree<byte[]> tree = indexContext.getTree();
        tree.put(key, value);
        return ackResponse(request);
    }

    @Override
    public Response handleCommit(CommitBalancingRequest request) {
        PhTree<byte[]> tree = indexContext.getTree();
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