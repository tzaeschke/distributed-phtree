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
package ch.ethz.globis.distindex.serializer;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;

import org.junit.Test;

import ch.ethz.globis.distindex.mapping.util.TestOperationsUtil;
import ch.ethz.globis.phtree.PhTree;

public class IterativeSerializerTest {

    @Test
    public void exportPhTree() throws FileNotFoundException {
        int dim = 6;
        int size = 100000;
        PhTree<String> tree = TestOperationsUtil.createRandomPhTree(size, dim);
        String filename = "tree.txt";
        IterativeSerializer<String> serializer = new IterativeSerializer<>();

        serializer.export(tree, filename);

        PhTree<String> outputTree = PhTree.create(dim);
        serializer.setTree(outputTree);

        PhTree<String> newTree = serializer.load(filename);

        assertEquals(tree.toStringPlain(), newTree.toStringPlain());
    }
}
