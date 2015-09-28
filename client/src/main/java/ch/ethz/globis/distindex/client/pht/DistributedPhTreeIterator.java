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
package ch.ethz.globis.distindex.client.pht;

import org.jboss.netty.handler.codec.replay.UnreplayableOperationException;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexIterator;
import ch.ethz.globis.pht.PhEntry;
import ch.ethz.globis.pht.PhTree.PhQuery;

/**
 * Main implementation of the PH Tree key-value iterator, backed by a proxy iterator.
 *
 * @param <V>                                   The value class for the iterator.
 */
public class DistributedPhTreeIterator<V> implements PhQuery<V> {

    private IndexIterator<long[], V> proxyIterator;

    public DistributedPhTreeIterator(IndexIterator<long[], V> proxyIterator) {
        this.proxyIterator = proxyIterator;
    }

    @Override
    public long[] nextKey() {
        return proxyIterator.next().getKey();
    }

    @Override
    public V nextValue() {
        return proxyIterator.next().getValue();
    }

    @Override
    public PhEntry<V> nextEntry() {
        IndexEntry<long[], V> currentEntry = proxyIterator.next();
        return new PhEntry<>(currentEntry.getKey(), currentEntry.getValue());
    }

    @Override
    public boolean hasNext() {
        return proxyIterator.hasNext();
    }

    @Override
    public V next() {
        return proxyIterator.next().getValue();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove operation not currently supported.");
    }

	@Override
	public void reset(long[] min, long[] max) {
		// TODO Auto-generated method stub
		throw new UnreplayableOperationException();
	}

	@Override
	public PhEntry<V> nextEntryReuse() {
		// TODO Auto-generated method stub
		return nextEntry();
	}
}
