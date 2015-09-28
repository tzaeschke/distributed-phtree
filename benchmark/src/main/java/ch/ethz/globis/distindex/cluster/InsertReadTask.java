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
package ch.ethz.globis.distindex.cluster;

import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.pht.nv.PhTreeNV;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class InsertReadTask implements Runnable {

    private PhTreeNV tree;
    private int nrEntries;

    public InsertReadTask(PHFactory factory, int nrEntries, int dim, int depth) {
        this.tree = factory.createPHTreeSet(dim, depth);
        this.nrEntries = nrEntries;
    }

    @Override
    public void run() {
        work(tree, nrEntries);
    }

    private void work(PhTreeNV tree, int nrEntries) {
        List<long[]> entries = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < nrEntries; i++) {
            entries.add(new long[]{
                    gaussianRandomValue(random), gaussianRandomValue(random)
            });
        }

        doWork(tree, entries);
    }

    private long gaussianRandomValue(Random random) {
        double r = random.nextGaussian();
        return (long) ((Long.MAX_VALUE - 1) * r);
    }

    private boolean doWork(PhTreeNV tree, List<long[]> points) {
        DateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start, end;
        boolean res = true;
        for (long[] point : points) {
            start = System.nanoTime();
            tree.insert(point);
            end = System.nanoTime();
            System.out.println(date.format(new Date()) + ",end,insert,"+ ((end - start) / 1000000.0));
            start = System.nanoTime();
            res &= tree.contains(point);
            end = System.nanoTime();
            System.out.println(date.format(new Date()) + ",end,get,"+ ((end - start) / 1000000.0));
        }
        return res;
    }
}