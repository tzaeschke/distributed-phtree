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

import java.util.concurrent.atomic.AtomicBoolean;

import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.v5.PhOperationsOL_COW;
import ch.ethz.globis.pht.v5.PhTree5;

/**
 * The in-memory context associated with the index.
 */
public class IndexContext {

    /** The current in-memory tree. */
    private PhTree<byte[]> tree;

    /** The cluster service, connected to the ZooKeeper cluster. */
    private ClusterService<long[]> clusterService;

    /** The host information on which the middleware is running. */
    private final String host;

    /** The port on which the middleware is running. */
    private final int port;

    /** The hostId of the current middleware, currently host:port */
    private final String hostId;

    /** The version of the last balancing. */
    private int lastBalancingVersion = 0;

    private AtomicBoolean isBalancing = new AtomicBoolean(false);

    public IndexContext(String host, int port) {
        this.host = host;
        this.port = port;
        this.hostId = host + ":" + port;
    }

    public void initTree(int dim, int depth) {
//        if (tree != null) {
//            return;
//        }
        PhTree5<byte[]> concurrentTree =  new PhTree5<>(dim, depth);
        concurrentTree.setOperations(new PhOperationsOL_COW<>(concurrentTree));
        this.tree = concurrentTree;
    }


    public PhTree<byte[]> getTree() {
        return tree;
    }

    public ClusterService<long[]> getClusterService() {
        return clusterService;
    }

    public void setClusterService(ClusterService<long[]> clusterService) {
        this.clusterService = clusterService;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getHostId() {
        return hostId;
    }

    public int getLastBalancingVersion() {
        return lastBalancingVersion;
    }

    public void setLastBalancingVersion(int lastBalancingVersion) {
        this.lastBalancingVersion = lastBalancingVersion;
    }

    public boolean canStartBalancing() {
        return isBalancing.compareAndSet(false, true);
    }

    public boolean endBalancing() {
        return isBalancing.compareAndSet(true, false);
    }

    public boolean isBalancing() {
        return isBalancing.get();
    }
}