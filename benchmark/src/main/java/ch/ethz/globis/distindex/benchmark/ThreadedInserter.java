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
package ch.ethz.globis.distindex.benchmark;

import ch.ethz.globis.pht.PhTree;

import java.util.List;
import java.util.concurrent.Callable;

public class ThreadedInserter implements Callable<Result> {

    private final int startIndex;
    private final int endIndex;
    private final PhTree<Object> tree;
    private final List<long[]> entries;

    ThreadedInserter(int startIndex, int endIndex, PhTree<Object> tree, List<long[]> entries) {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.tree = tree;
        this.entries = entries;
    }

    @Override
    public Result call() throws Exception {
        double averageResponseTime = 0;
        long start = System.nanoTime();

        long s, e;
        for (int i = startIndex; i < endIndex; i++) {
            s = System.nanoTime();
            tree.put(entries.get(i), null);
            e = System.nanoTime();
            averageResponseTime += (e - s) / 1000000.0;
        }
        long end = System.nanoTime();
        int nrEntries = endIndex - startIndex;
        averageResponseTime /= nrEntries;
        start /= 1000000.0;
        end /= 1000000.0;
        return new Result(start, end, nrEntries, averageResponseTime);
    }
}
