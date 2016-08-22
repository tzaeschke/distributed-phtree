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
package ch.ethz.globis.distindex.middleware.balancing;

import ch.ethz.globis.distindex.middleware.IndexContext;
import ch.ethz.globis.distindex.middleware.PhTreeRequestHandler;
import ch.ethz.globis.phtree.PhTree;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BalancingDaemon implements Runnable {

    /** The index context associated with this handler. */
    private IndexContext indexContext;
    /** The balancing strategy used */
    private BalancingStrategy balancingStrategy;

    /** Flag indicating whether the Daemon is running or was stopped. */
    private boolean running;

    /** The period in which the Daemon is running. */
    private long period;

    private ScheduledExecutorService service = new ScheduledThreadPoolExecutor(1);

    public BalancingDaemon(IndexContext indexContext, BalancingStrategy balancingStrategy, long period) {
        this.running = true;
        this.period = period;
        this.balancingStrategy = balancingStrategy;
        this.indexContext = indexContext;
    }

    @Override
    public void run() {
        if (running) {
            service.schedule(new BalancingCheckerTask(this), period, TimeUnit.MILLISECONDS);
        }
    }

    public void close() {
        this.running = false;
        this.service.shutdown();
    }

    public void balanceAndRemove() {
        balancingStrategy.balanceAndRemove();
        close();
    }

    class BalancingCheckerTask implements Runnable {

        BalancingDaemon parent;
        public BalancingCheckerTask(BalancingDaemon balancingDaemon) {
            parent = balancingDaemon;
        }

        @Override
        public void run() {
            try {
                checkBalancing();
            } finally {
                parent.run();
            }
        }

        private void checkBalancing() {
            PhTree<byte[]> tree = indexContext.getTree();
            if (tree.size() > PhTreeRequestHandler.THRESHOLD) {
                balancingStrategy.balance();
            }
        }
    }
}
