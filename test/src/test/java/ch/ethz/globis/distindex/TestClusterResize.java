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
package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.middleware.PhTreeIndexMiddlewareFactory;
import ch.ethz.globis.distindex.middleware.net.IndexMiddleware;
import ch.ethz.globis.distindex.middleware.util.MiddlewareUtil;
import org.apache.curator.test.TestingServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class TestClusterResize {

    private static final Logger LOG = LoggerFactory.getLogger(TestClusterResize.class);
    private static final String HOST = "localhost";
    private static final String ZK_HOST = HOST;
    private static final int ZK_PORT = 2181;
    private static int middlewareCounter = 0;
    private static final int S_BASE_PORT = 7070;
    private static final ExecutorService service = Executors.newFixedThreadPool(8);

    @Test
    public void testRemoveLeftEnd() {
        IndexMiddleware<long[], byte[]>[] middlewares = new IndexMiddleware[] {
                createMiddleware(false), createMiddleware(false), createMiddleware(false)
        };
        int dim = 2;
        int depth = 64;
        int size = 50;
        try (TestingServer server = new TestingServer(ZK_PORT);
            PHTreeIndexProxy<Object> tree = new PHTreeIndexProxy<>(ZK_HOST, ZK_PORT)) {
            server.start();
            startMiddlewares(middlewares);
            tree.create(dim, depth);
            IndexEntryList<long[], Object> entries = createEntries(dim, size);
            insert(tree, entries);

            System.out.println("Mapping: " + tree.getMapping());
            assertTrue(contains(tree, entries));

            middlewares[1].remove();
            assertTrue(contains(tree, entries));
        } catch (Exception e) {
            LOG.error("Exception running the test!", e);
            fail();
        } finally {
            closeMiddlewares(middlewares);
        }
    }

    @Test
    public void testRemoveRightEnd() {
        IndexMiddleware<long[], byte[]>[] middlewares = new IndexMiddleware[] {
                createMiddleware(false), createMiddleware(false), createMiddleware(false)
        };
        int dim = 2;
        int depth = 64;
        int size = 50;
        try (TestingServer server = new TestingServer(ZK_PORT);
             PHTreeIndexProxy<Object> tree = new PHTreeIndexProxy<>(ZK_HOST, ZK_PORT)) {
            server.start();
            startMiddlewares(middlewares);
            tree.create(dim, depth);
            IndexEntryList<long[], Object> entries = createEntries(dim, size);
            insert(tree, entries);

            System.out.println("Mapping: " + tree.getMapping());
            assertTrue(contains(tree, entries));

            middlewares[2].remove();
            assertTrue(contains(tree, entries));
        } catch (Exception e) {
            LOG.error("Exception running the test!", e);
            fail();
        } finally {
            closeMiddlewares(middlewares);
        }
    }

    @Test
    public void testRemoveMiddle() {
        IndexMiddleware<long[], byte[]>[] middlewares = new IndexMiddleware[] {
                createMiddleware(false), createMiddleware(false), createMiddleware(false)
        };
        int dim = 2;
        int depth = 64;
        int size = 50;
        try (TestingServer server = new TestingServer(ZK_PORT);
             PHTreeIndexProxy<Object> tree = new PHTreeIndexProxy<>(ZK_HOST, ZK_PORT)) {
            server.start();
            startMiddlewares(middlewares);
            tree.create(dim, depth);
            IndexEntryList<long[], Object> entries = createEntries(dim, size);
            insert(tree, entries);

            System.out.println("Mapping: " + tree.getMapping());
            assertTrue(contains(tree, entries));

            middlewares[0].remove();
            assertTrue(contains(tree, entries));
        } catch (Exception e) {
            LOG.error("Exception running the test!", e);
            fail();
        } finally {
            closeMiddlewares(middlewares);
        }
    }

    @Test
    public void testRemoveToFree() {
        IndexMiddleware<long[], byte[]>[] middlewares = new IndexMiddleware[] {
                createMiddleware(false), createMiddleware(false), createMiddleware(false), createMiddleware(true)
        };
        int dim = 2;
        int depth = 64;
        int size = 50;
        try (TestingServer server = new TestingServer(ZK_PORT);
             PHTreeIndexProxy<Object> tree = new PHTreeIndexProxy<>(ZK_HOST, ZK_PORT)) {
            server.start();
            startMiddlewares(middlewares);
            tree.create(dim, depth);
            IndexEntryList<long[], Object> entries = createEntries(dim, size);
            insert(tree, entries);

            System.out.println("Mapping before: " + tree.getMapping());
            assertTrue(contains(tree, entries));

            middlewares[0].remove();

            System.out.println("Mapping after: " + tree.getMapping());
            assertTrue(contains(tree, entries));
        } catch (Exception e) {
            LOG.error("Exception running the test!", e);
            fail();
        } finally {
            closeMiddlewares(middlewares);
        }
    }

    @Test
    public void testRemoveAllToFree() {
        IndexMiddleware<long[], byte[]>[] middlewares = new IndexMiddleware[] {
                createMiddleware(false), createMiddleware(false), createMiddleware(false), createMiddleware(true)
        };
        int dim = 2;
        int depth = 64;
        int size = 50;
        try (TestingServer server = new TestingServer(ZK_PORT);
             PHTreeIndexProxy<Object> tree = new PHTreeIndexProxy<>(ZK_HOST, ZK_PORT)) {
            server.start();
            startMiddlewares(middlewares);
            tree.create(dim, depth);
            IndexEntryList<long[], Object> entries = createEntries(dim, size);
            insert(tree, entries);

            System.out.println("Mapping before: " + tree.getMapping());
            assertTrue(contains(tree, entries));

            middlewares[0].remove();
            middlewares[1].remove();
            middlewares[2].remove();
            System.out.println("Mapping after: " + tree.getMapping());
            assertTrue(contains(tree, entries));
        } catch (Exception e) {
            LOG.error("Exception running the test!", e);
            fail();
        } finally {
            closeMiddlewares(middlewares);
        }
    }


    private IndexEntryList<long[], Object> createEntriesRandomly(int dim, int size) {
        Random random = new Random();
        IndexEntryList<long[], Object> entries = new IndexEntryList<>();

        for (int i  = 0; i < size; i++) {
            long[] key = new long[dim];
            for (int d = 0; d <  dim; d++) {
                key[d] = i - (size / 2);
            }
            Object value = new BigInteger(64, random);
            entries.add(key, value);
        }
        return entries;
    }

    private IndexEntryList<long[], Object> createEntries(int dim, int size) {
        Random random = new Random();
        IndexEntryList<long[], Object> entries = new IndexEntryList<>();

        for (int i  = 0; i < size; i++) {
            long[] key = new long[dim];
            for (int d = 0; d <  dim; d++) {
                key[d] = random.nextLong();
            }
            Object value = new BigInteger(64, random);
            entries.add(key, value);
        }
        return entries;
    }

    private void insert(PHTreeIndexProxy<Object> tree, IndexEntryList<long[], Object> entries) {
        for (IndexEntry<long[], Object> entry :  entries) {
            tree.put(entry.getKey(), entry.getValue());
        }
    }

    private boolean contains(PHTreeIndexProxy<Object> tree, IndexEntryList<long[], Object> entries) {
        Object val;
        for (IndexEntry<long[], Object> entry :  entries) {
            val = tree.get(entry.getKey());
            if (val == null || (!val.equals(entry.getValue()))) {
                return false;
            }
        }
        return true;
    }

    private void startMiddlewares(IndexMiddleware[] middlewares) {
        for (IndexMiddleware middleware : middlewares) {
            MiddlewareUtil.startMiddleware(service, middleware);
        }
    }

    private void closeMiddlewares(IndexMiddleware[] middlewares) {
        for (IndexMiddleware middleware : middlewares) {
            if (middleware.isRunning()) {
                middleware.close();
            }
        }
    }

    private IndexMiddleware<long[], byte[]> createMiddleware(boolean free) {
        IndexMiddleware<long[], byte[]> middleware =
                PhTreeIndexMiddlewareFactory.newPhTree(HOST, S_BASE_PORT + 2 * middlewareCounter++, ZK_HOST, ZK_PORT);
        middleware.setJoinedAsFree(free);
        return middleware;
    }
}
