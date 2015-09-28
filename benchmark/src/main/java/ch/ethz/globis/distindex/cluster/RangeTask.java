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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.pht.nv.PhTreeNV;

public class RangeTask implements Runnable{

    private PhTreeNV tree;
    private int nrEntries;

    public RangeTask(PHFactory factory, int nrEntries, int dim, int depth) {
        this.tree = factory.createPHTreeSet(dim, depth);
        this.nrEntries = nrEntries;
    }

    @Override
    public void run() {
        insert(tree, nrEntries);
    }

    private void insert(PhTreeNV tree, int nrEntries) {
        List<long[]> entries = new ArrayList<long[]>();

        Random random = new Random(42);
        for (int i = 0; i < nrEntries; i++) {
            entries.add(new long[]{
                    gaussianRandomValue(random), gaussianRandomValue(random)
            });
        }

        doInsert(tree, entries);
    }

    private long gaussianRandomValue(Random random) {
        double r = random.nextGaussian();
        return (long) ((Short.MAX_VALUE * 128) * r);
    }

    private void doInsert(PhTreeNV tree, List<long[]> points) {
        DateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start, end;
        for (long[] point : points) {
            tree.insert(point);
        }

        long offsetA, offsetB;
        long[] keyStart;
        long[] keyEnd;
        Random r = new Random();

        /*
         *  offsetA = r.nextInt(Short.MAX_VALUE * 3);
            offsetB = r.nextInt(Short.MAX_VALUE * 3);

            and

            long[] key = {0, 0};

            for each query creates queries that always hit all the hosts
         */
        long[] key = {0, 0};
        for (int i = 0; i < nrEntries / 5; i++) {
            offsetA = r.nextInt(Short.MAX_VALUE * 32);
            offsetB = r.nextInt(Short.MAX_VALUE * 32);

            key = points.get(i);
            keyStart = new long[] {key[0] - offsetA, key[1] - offsetA};
            keyEnd = new long[] { key[0] + offsetB, key[1] + offsetB};

            start = System.nanoTime();
            List<?> objects = tree.queryAll(keyStart, keyEnd);
            end = System.nanoTime();
            System.out.println(date.format(new Date()) + ",end,range,"+ ((end - start) / 1000000.0) + "," + objects.size());
        }
    }
}