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
package ch.ethz.globis.distindex.client;

import ch.ethz.globis.distindex.mapping.KeyMapping;
import ch.ethz.globis.distindex.util.CollectionUtil;

import java.util.*;

/**
 * This class is a mock and is only used during testing.
 *
 * Might want to even remove it.
 * @param <K>
 */
@Deprecated
public class NonDistributedMapping<K> implements KeyMapping<K> {

    private final String host;
    private final List<String> hostList;

    public NonDistributedMapping(String host) {
        this.host = host;
        this.hostList = new ArrayList<>();
        hostList.add(host);
    }

    public Map<String, String> asMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String get(K key) {
        return host;
    }

    @Override
    public List<String> get(K start, K end) {
        return hostList;
    }

    public List<String> getHostIds(String prefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> get() {
        return hostList;
    }

    @Override
    public String getFirst() {
        return hostList.get(0);
    }

    @Override
    public String getNext(String hostId) {
        int index = CollectionUtil.search(hostList, hostId) + 1;
        if (index >= hostList.size()) {
            return null;
        } else {
            return hostList.get(index + 1);
        }
    }

    @Override
    public String getPrevious(String hostId) {
        int index = CollectionUtil.search(hostList, hostId) - 1;
        if (index < 0) {
            return null;
        } else {
            return hostList.get(index + 1);
        }
    }

    @Override
    public void add(String hostId) {
        int index = CollectionUtil.search(hostList, hostId);
        hostList.add(index, hostId);
    }

    @Override
    public void remove(String hostId) {
        int index = CollectionUtil.search(hostList, hostId);
        hostList.remove(index);
    }

//    @Override
//    public void setSize(String host, int size) {
//        //do nothing
//    }
//
//    @Override
//    public int getSize(String host) {
//        return 0;
//    }

    public String getHostForSplitting(String currentHostId) {
        throw new UnsupportedOperationException();
    }

    public int getVersion() {
        return 0;
    }

    public void setVersion(int version) {

    }

    @Override
    public int size() {
        return hostList.size();
    }

    @Override
    public void clear() {
    }
}