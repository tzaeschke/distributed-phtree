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
import ch.ethz.globis.distindex.client.pht.ZKPHFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClusterMixedBenchmark {

    private static String ZK_HOST = "localhost";
    private static int ZK_PORT = 2181;
    private static int NR_ENTRIES = 50000;
    private static int NR_THREADS = 4;

    public static void main(String[] args) {
        extractArguments(args);

        PHFactory factory = new ZKPHFactory(ZK_HOST, ZK_PORT);
        int dim = 2;
        int depth = 64;
        int nrEntries = NR_ENTRIES;

        workloadWithClients(factory, nrEntries, dim, depth);
        factory.close();
    }

    private static void workloadWithClients(PHFactory factory, int nrEntries, int dim, int depth) {
        int nrClients = NR_THREADS;
        ExecutorService pool = Executors.newFixedThreadPool(nrClients);

        List<Runnable> tasks = new ArrayList<Runnable>();
        try {
            for (int i = 0; i < nrClients / 2; i++) {
                tasks.add(new InsertionTask(factory, nrEntries, dim, depth));
            }
            for (int i = nrClients / 2; i < nrClients; i++) {
                tasks.add(new ReadTask(factory, nrEntries, dim, depth));
            }
            for (Runnable task : tasks) {
                pool.execute(task);
            }

            pool.shutdown();
            pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void extractArguments(String[] args) {
        if (args.length > 0) {
            ZK_HOST = args[0];
        }
        if (args.length > 1) {
            ZK_PORT = Integer.valueOf(args[1]);
        }
        if (args.length > 2) {
            NR_ENTRIES = Integer.valueOf(args[2]);
        }
        if (args.length > 3) {
            NR_THREADS = Integer.valueOf(args[3]);
        }
    }
}
