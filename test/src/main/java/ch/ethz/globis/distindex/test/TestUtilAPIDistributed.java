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
package ch.ethz.globis.distindex.test;

import ch.ethz.globis.distindex.client.pht.DistributedPhTreeV;
import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.client.pht.ZKPHFactory;
import ch.ethz.globis.distindex.middleware.api.Middleware;
import ch.ethz.globis.distindex.middleware.PhTreeIndexMiddlewareFactory;
import ch.ethz.globis.distindex.middleware.util.MiddlewareUtil;
import ch.ethz.globis.pht.PhTree;
import ch.ethz.globis.pht.nv.PhTreeNV;
import ch.ethz.globis.pht.nv.PhTreeVProxy;
import ch.ethz.globis.pht.test.util.TestUtilAPI;

import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Implementation of the TestUtilAPI
 */
public class TestUtilAPIDistributed implements TestUtilAPI {

    /** Current logged */
    private static final Logger LOG = LoggerFactory.getLogger(TestUtilAPIDistributed.class);

    /** The host used for zookeeper */
    private static final String ZK_HOST = "localhost";

    /** The port used for zookeeper */
    private static final int ZK_PORT = 2181;

    /** The base port used when creating index servers */
    private static final int S_BASE_PORT = 7070;

    /** Thread pool used to run the index servers */
    private static ExecutorService threadPool;

    /** References to the index servers */
    private static List<Middleware> middlewares = new ArrayList<>();

    /** Zookeeper server*/
    private static TestingServer zkServer;
    private List<DistributedPhTreeV> trees = new ArrayList<>();

    private int nrServers;

    public TestUtilAPIDistributed(int nrServers) throws IOException {
        this.nrServers = nrServers;
        beforeSuite();
    }

    private ZKPHFactory factory = new ZKPHFactory(ZK_HOST, ZK_PORT);

    @Override
    public PhTreeNV newTree(int dim, int depth) {
        return new PhTreeVProxy(newTreeV(dim, depth));
    }

    @Override
    public <T> PhTree<T> newTreeV(int dim, int depth) {

        PhTree<T> tree = factory.createPHTreeMap(dim, depth);
        trees.add((DistributedPhTreeV) tree);
        return tree;
    }

    @Override
    public void close(PhTreeNV phTree) {
        //currently not needed
    }

    @Override
    public <T> void close(PhTree<T> tPhTreeV) {
        //currently not needed
    }

    @Override
    public void beforeTest() {
        System.out.println("Clearing trees.");
        try {
            for (DistributedPhTreeV tree : trees) {
                tree.getProxy().close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            trees.clear();
        }
        //currently not needed
    }

    @Override
    public void beforeTest(Object[] objects) {
        //currently not needed
    }

    @Override
    public void afterTest() {
        //currently not needed
    }

    @Override
    public void beforeSuite() {
        try {
            threadPool = Executors.newFixedThreadPool(32);
            startZK();

            for (Middleware middleware : middlewares) {
                middleware.close();
            }
            middlewares.clear();

            for (int i = 0; i < nrServers; i++) {
                Middleware current = PhTreeIndexMiddlewareFactory.newPhTree(ZK_HOST, S_BASE_PORT + i * 10, ZK_HOST, ZK_PORT);
                MiddlewareUtil.startMiddleware(threadPool, current);
                middlewares.add(current);
            }
        } catch (Exception e) {
            LOG.error("Failed to start test suite. ", e);
        }
    }

    @Override
    public void afterSuite() {
        try {
            for (Middleware middleware : middlewares) {
                middleware.close();
            }
            threadPool.shutdown();
            stopZK();
        } catch (Exception e) {
            LOG.error("Exception during suite shutdown. ", e);
        }
    }

    private void startZK() {
        try {
            zkServer = new TestingServer(ZK_PORT);
            zkServer.start();
        } catch (Exception e) {
            LOG.warn("Cannot open testing ZK. Attempting to use possible running ZK");
        }
    }

    private void stopZK() {
        try {
            zkServer.stop();
        } catch (NullPointerException npe) {
            LOG.error("ZK was not initialized.");
        } catch (IOException e) {
            LOG.error("Failed to close ZK.", e);
        }
    }

    @Override
    public void beforeClass() {
        //currently not needed
    }

    @Override
    public void afterClass() {
        //currently not needed
    }
}