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
package ch.ethz.globis.distindex.mapping;

import java.util.List;

public interface KeyMapping<K> {

    /**
     * Add a hostId to the key mapping.
     * @param host
     */
    public void add(String host);

    /**
     * Remove a hostId from the keyMapping
     * @param host
     */
    public void remove(String host);

    /**
     * Obtain the hostId of the machine that stores the key received as an argument.
     *
     * @param key
     * @return
     */
    public String get(K key);

    /**
     * Obtain all hostIds that store keys between the range determined by the start and end keys
     * received as arguments.
     * @param start
     * @param end
     * @return
     */
    public List<String> get(K start, K end);

    /**
     * Obtain all the host ids.
     * @return
     */
    public List<String> get();

    /**
     * Get the hostId of the host that holds the first key interval.
     * @return
     */
    public String getFirst();

    /**
     * Get the hostUId of the host that holds the next key interval relative to the key interval
     * of the hostId received as an argument.
      * @param hostId
     * @return
     */
    public String getNext(String hostId);

    /**
     * Return the host id of the host preceding the host whose id was received as an argument.
     * @param hostId
     * @return
     */
    public String getPrevious(String hostId);

    /**
     * Return the number of hosts within the mapping.
     * @return
     */
    public int size();

    /**
     * Clear the mapping.
     */
    public void clear();

    /**
     * @return                          The version of the mapping.
     */
    public int getVersion();

    /**
     * Update the current mapping version.
     * @param version                   The new version of the mapping.
     */
    public void setVersion(int version);
}