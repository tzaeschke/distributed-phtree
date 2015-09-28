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
package ch.ethz.globis.distindex.orchestration;

import ch.ethz.globis.distindex.mapping.KeyMapping;

import java.util.List;
import java.util.Map;

public interface ClusterService<K> {

    /**
     * Connect to the cluster.
     */
    public void connect();

    /**
     * Disconnect from the cluster.
     */
    public void disconnect();

    /**
     * Register the host with the hostId as an index peer.
     * @param hostId
     */
    public void registerHost(String hostId);

    /**
     * Register the host with the hostId as an index peer.
     * @param hostId
     */
    public void deregisterHost(String hostId);

    /**
     * Register the host as a free host. This host will not be part of the cluster until a host
     * that is full will pick it.
     *
     * @param hostId                    The id of the host to be marked as free.
     */
    public void registerFreeHost(String hostId);

    /**
     * Remove a free host from the list of free hosts and return it to the list of free hosts.
     *
     * @return                          The id of the removed host.
     */
    public String getNextFreeHost();

    /**
     * @return                              The online hosts registered in the cluster.
     */
    public List<String> getOnlineHosts();

    /**
     * Handle the creation of a new index.
     *
     * @param options
     */
    public void createIndex(Map<String, String> options);

    /**
     * Reads the current key-to-machine mapping from the distributed cluster configuration.
     * @return                              The current cluster configuration.
     */
    public KeyMapping<K> getMapping();

    /**
     * Returns an approximation of the number of entries stored by a host. This is the latest value that
     * the client read from Zookeeper.
     *
     * @param hostId                        The id of the host.
     * @return                              The number of entries of a host.
     */
    public int getSize(String hostId);

    /**
     * Change the number of entries associated with a host in ZooKeeper.
     * @param hostId
     * @param size
     */
    public void setSize(String hostId, int size);
}