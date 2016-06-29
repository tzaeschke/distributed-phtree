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
package ch.ethz.globis.distindex.concurrency.dummies;

import ch.ethz.globis.pht.PhTree;

/**
 * Contains the tree modification operations: insert, remove and update key.
 *
 *  The put, delete and update operations we decomposed into several methods. These methods
 *  are split into two categories:
 *  - navigation methods: these are only going down the tree and don't perform any modifications
 *  - mutation methods: these methods receive some nodes as arguments and modify them
 *
 *  This split is useful for extending insert, remove and update operations for multi-threading.
 *
 * @param <T>
 */
public class PhOperationsSimple<T> implements PhOperations<T> {

    protected static final int NO_INSERT_REQUIRED = Integer.MAX_VALUE;

    protected PhTree<T> tree;

    PhOperationsSimple() {}

    public PhOperationsSimple(PhTree<T> tree) {
        this.tree = tree;
    }

    @Override
    public Node<T> createNode(Node<T> original, int dim) {
    	throw new UnsupportedOperationException();
    }

    @Override
	public Node<T> createNode(PhTree<T> parent, int infixLen, int postLen, 
			int estimatedPostCount, final int DIM) {
    	throw new UnsupportedOperationException();
	}

    @Override
    public T put(long[] key, T value) {
    	return tree.put(key, value);
    }

    public void setTree(PhTreeC<T> tree) {
        this.tree = tree;
    }

	@Override
	public T remove(long... key) {
		return tree.remove(key);
	}

	@Override
	public T update(long[] oldKey, long[] newKey) {
		return tree.update(oldKey, newKey);
	}
}
