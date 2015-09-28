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
package ch.ethz.globis.distindex.api;

/**
 * Representing an index that associates key objects to value objects.
 *
 * @param <K>                           The type of the index key.
 * @param <V>                           The type of the index value.
 */
public interface Index<K, V> {

    /**
     * Add an entry into the index, replacing the previous value with the same key.
     *
     * @param key                       The key of the new index entry.
     * @param value                     The value of the new index entry.
     * @return                          The previous value associated with the key or null of there was no
     *                                  previous value associated with this key.
     */
    public V put(K key, V value);

    /**
     * Check if the index contains a value associated with this key.
     * @param key                       The key to be searched.
     * @return                          True if there is a value in the index associated with this key, false otherwise.
     */
    public boolean contains(K key);

    /**
     * Retrieves a value from the index based on a key.
     * @param key                       The query key.
     * @return                          The value associated with the query key or null if there is no value associated
     *                                  with the query key.
     */
    public V get(K key);

    /**
     * Removes an entry from the index.
     *
     * @param key                       The query key.
     * @return                          The previous value associated with the key or null of there was no
     *                                  previous value associated with this key.
     */
    public V remove(K key);

    @Deprecated
    public IndexEntryList<K, V> getRange(K start, K end);

    /**
     * @return                          An iterator over all the entries in the index.
     */
    public IndexIterator<K, V> iterator();


    /**
     * Obtain an iterator over all the entries in the index that fall in the range [start, end] according to
     * the key ordering.
     * @param start                     The start of the key range.
     * @param end                       The end of the key range.
     * @return                          An iterator over the matched range.
     */
    public IndexIterator<K, V> query(K start, K end);
}
