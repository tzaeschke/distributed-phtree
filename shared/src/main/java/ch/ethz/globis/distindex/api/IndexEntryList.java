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

import java.util.ArrayList;

/**
 * An ordered list of {@link ch.ethz.globis.distindex.api.IndexEntry} objects.
 *
 * @param <K>                           The type of the index key.
 * @param <V>                           The type of the index value.
 */
public class IndexEntryList<K, V> extends ArrayList<IndexEntry<K, V>> {

    /** */
	private static final long serialVersionUID = 1L;

	public IndexEntryList() {
        super();
    }

    public IndexEntryList(int size) {
        super(size);
    }

    public IndexEntryList(IndexEntry<K, V> entry) {
        super();
        add(entry);
    }

    public IndexEntryList(K key, V value) {
        super();
        add(new IndexEntry<>(key, value));
    }

    public void add(K key, V value) {
        add(new IndexEntry<>(key, value));
    }
}