package ch.ethz.globis.distindex;

import ch.ethz.globis.distindex.api.IndexEntry;
import ch.ethz.globis.distindex.api.IndexEntryList;
import ch.ethz.globis.distindex.client.DistributedIndexIterator;
import ch.ethz.globis.distindex.client.DistributedIndexRangedIterator;
import ch.ethz.globis.distindex.client.pht.DistributedPHTree;
import ch.ethz.globis.distindex.middleware.net.IndexMiddlewareFactory;
import ch.ethz.globis.distindex.middleware.api.Middleware;
import ch.ethz.globis.distindex.util.MultidimUtil;
import org.apache.curator.test.TestingServer;
import org.junit.*;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class DistributedPHTreeTest {

    private static final int ZK_PORT = 2181;
    private static ExecutorService threadPool;

    @Before
    public void initExecutor() {
        threadPool = Executors.newFixedThreadPool(2);
    }

    @After
    public void shutdownExecutor() throws InterruptedException {
        threadPool.shutdownNow();
        threadPool.awaitTermination(10, TimeUnit.SECONDS);
    }

    @Test
    public void testLargeValues() throws Exception {
        int dim = 2;
        int depth = 64;

        String host = "localhost";

        try (TestingServer zkServer  = new TestingServer(ZK_PORT);
             Middleware middleware = IndexMiddlewareFactory.newPHTreeMiddleware(7070)) {
            zkServer.start();
            startMiddleware(middleware);

            DistributedPHTree<String> phTree = new DistributedPHTree<>(host, ZK_PORT, String.class);
            phTree.create(dim, depth);

            long[] key = {1L, 2L};
            String veryLargeString = new BigInteger(1024 * 1024, new Random()).toString();

            phTree.put(key, veryLargeString);
            assertEquals(veryLargeString, phTree.get(key));
            phTree.close();
        }
    }

    @Test
    public void testGet() throws Exception {
        int dim = 2;
        int depth = 64;

        String host = "localhost";

        try (TestingServer zkServer  = new TestingServer(ZK_PORT);
            Middleware middleware = IndexMiddlewareFactory.newPHTreeMiddleware(7070)) {
            zkServer.start();
            startMiddleware(middleware);

            DistributedPHTree<String> phTree = new DistributedPHTree<>(host, ZK_PORT, String.class);
            phTree.create(dim, depth);

            String retrieved = phTree.get(new long[] { 1L, 2L});

            assertNull("Retrieved value should be null as it is not in the tree.", retrieved);
            phTree.close();
        }
    }

    @Test
    public void testRangedIterator() throws Exception {
        int dim = 2;
        int depth = 64;
        String host = "localhost";
        try (TestingServer zkServer  = new TestingServer(ZK_PORT);
             Middleware first = IndexMiddlewareFactory.newPHTreeMiddleware(7070);
             Middleware second = IndexMiddlewareFactory.newPHTreeMiddleware(8080)) {
            zkServer.start();
            startMiddleware(first);
            startMiddleware(second);

            DistributedPHTree<String> phTree = new DistributedPHTree<>(host, ZK_PORT, String.class);
            phTree.create(dim, depth);

            IndexEntryList<long[], String> expected = new IndexEntryList<>();
            IndexEntryList<long[], String> toInsert = new IndexEntryList<>();
            toInsert.add(k(0, 1), "foo");
            expected.add(k(0, 1), "foo");
            toInsert.add(k(1, 2), "bar");
            toInsert.add(k(-1, -2), "fuzz");
            toInsert.add(k(-1, -1), "fuz");
            expected.add(k(-1, -1), "fuz");

            for (IndexEntry<long[], String> entry : toInsert) {
                phTree.put(entry.getKey(), entry.getValue());
            }

            Iterator<IndexEntry<long[], String>> it = phTree.query(k(-1, -1), k(1, 1));

            IndexEntryList<long[], String> received = new IndexEntryList<>();
            while (it.hasNext()) {
                IndexEntry<long[], String> entry = it.next();
                received.add(entry);
            }

            assertEquals(expected.size(), received.size());
            for (int i = 0; i < received.size(); i++) {
                assertArrayEquals(expected.get(i).getKey(), received.get(i).getKey());
                assertEquals(expected.get(i).getValue(), received.get(i).getValue());
            }
        }
    }

    @Test
    public void testIterator() throws Exception {
        int dim = 2;
        int depth = 64;
        String host = "localhost";
        try (TestingServer zkServer  = new TestingServer(ZK_PORT);
             Middleware first = IndexMiddlewareFactory.newPHTreeMiddleware(7070);
             Middleware second = IndexMiddlewareFactory.newPHTreeMiddleware(8080)) {
                zkServer.start();
                startMiddleware(first);
                startMiddleware(second);

                DistributedPHTree<String> phTree = new DistributedPHTree<>(host, ZK_PORT, String.class);
                phTree.create(dim, depth);

                IndexEntryList<long[], String> expected = new IndexEntryList<>();

                expected.add(k(0, 0), "foo1");
                expected.add(k(0, 1), "foo");
                expected.add(k(1, 1), "foo");
                expected.add(k(1, 2), "bar");
                expected.add(k(-1, -2), "fuzz");
                expected.add(k(-1, -1), "fuz");

                for (IndexEntry<long[], String> entry : expected) {
                    phTree.put(entry.getKey(), entry.getValue());
                }

                DistributedIndexIterator<long[], String> it = (DistributedIndexIterator<long[], String>) phTree.iterator();

                IndexEntryList<long[], String> received = new IndexEntryList<>();
                while (it.hasNext()) {
                    IndexEntry<long[], String> entry = it.next();
                    received.add(entry);
                }

                assertEquals(expected.size(), received.size());
                for (int i = 0; i < received.size(); i++) {
                    assertArrayEquals(expected.get(i).getKey(), received.get(i).getKey());
                    assertEquals(expected.get(i).getValue(), received.get(i).getValue());
                }
        }
    }

     @Test
    public void testGetRange() throws Exception {
        int dim = 2;
        int depth = 64;

        String host = "localhost";
        try (TestingServer zkServer  = new TestingServer(ZK_PORT);
             Middleware middleware = IndexMiddlewareFactory.newPHTreeMiddleware(7070)) {
            zkServer.start();
            startMiddleware(middleware);

            DistributedPHTree<String> phTree = new DistributedPHTree<>(host, ZK_PORT, String.class);
            phTree.create(dim, depth);

            phTree.put(new long[] {10, 10}, "foo");
            phTree.put(new long[] {11, 10}, "foo");
            phTree.put(new long[]{9, 10}, "foo");
            phTree.put(new long[]{10, 9}, "foo");
            phTree.put(new long[]{10, 11}, "foo");
            phTree.put(new long[]{10, 12}, "foo");
            phTree.put(new long[]{9, 7}, "foo");
            phTree.put(new long[]{10, 9}, "foo");
            phTree.put(new long[]{100000000, -1}, "foo");
            phTree.put(new long[]{-1, 100000000}, "foo");

            IndexEntryList<long[], String> result = phTree.getRange(new long[]{9, 9}, new long[]{11, 11});
            IndexEntryList<long[], String> expected = new IndexEntryList<>();
            expected.add(new IndexEntry<>(new long[] { 9L, 10L}, "foo"));
            expected.add(new IndexEntry<>(new long[] { 10L, 9L}, "foo"));
            expected.add(new IndexEntry<>(new long[] { 10L, 10L}, "foo"));
            expected.add(new IndexEntry<>(new long[] { 10L, 11L}, "foo"));
            expected.add(new IndexEntry<>(new long[] { 11L, 10L}, "foo"));

            assertEquals(result.size(), expected.size());
            for (int i = 0; i < result.size(); i++) {
                assertArrayEquals(expected.get(i).getKey(), result.get(i).getKey());
                assertEquals(expected.get(i).getValue(), result.get(i).getValue());
            }
            phTree.close();
        }
    }

    @Test
    public void testGetRange2Mid() throws Exception {
        int dim = 2;
        int depth = 64;

        String host = "localhost";
        try (TestingServer zkServer = new TestingServer(ZK_PORT);
             Middleware middleware = IndexMiddlewareFactory.newPHTreeMiddleware(7070);
             Middleware second = IndexMiddlewareFactory.newPHTreeMiddleware(7080)
        ) {
            zkServer.start();
            startMiddleware(middleware);
            startMiddleware(second);

            DistributedPHTree<String> phTree = new DistributedPHTree<>(host, ZK_PORT, String.class);
            phTree.create(dim, depth);

            phTree.put(new long[] {0, 0}, "foo");
            phTree.put(new long[] {1, 0}, "foo");
            phTree.put(new long[]{1, 1}, "foo");
            phTree.put(new long[]{-1, 0}, "foo");
            phTree.put(new long[]{0, 2}, "foo");
            phTree.put(new long[]{-2, 3}, "foo");
            phTree.put(new long[]{-2, 2}, "foo");

            IndexEntryList<long[], String> result = phTree.getRange(new long[]{-2, -2}, new long[]{2, 2});
            IndexEntryList<long[], String> expected = new IndexEntryList<>();
            expected.add(new IndexEntry<>(new long[] { 0L, 0L}, "foo"));
            expected.add(new IndexEntry<>(new long[] { 1L, 0L}, "foo"));
            expected.add(new IndexEntry<>(new long[] { 1L, 1L}, "foo"));
            expected.add(new IndexEntry<>(new long[] { 0L, 2L}, "foo"));
            expected.add(new IndexEntry<>(new long[] { -1L, 0L}, "foo"));
            expected.add(new IndexEntry<>(new long[] { -2L, 2L}, "foo"));

            assertEquals(result.size(), expected.size());
            for (int i = 0; i < result.size(); i++) {
                assertArrayEquals(expected.get(i).getKey(), result.get(i).getKey());
                assertEquals(expected.get(i).getValue(), result.get(i).getValue());
            }
            phTree.close();
        }
    }

    @Test
    public void testPutEmpty() throws Exception {
        int dim = 2;
        int depth = 64;

        String host = "localhost";
        try (TestingServer zkServer = new TestingServer(ZK_PORT);
             Middleware middleware = IndexMiddlewareFactory.newPHTreeMiddleware(7070);
             Middleware second = IndexMiddlewareFactory.newPHTreeMiddleware(7080)
        ) {
            zkServer.start();
            startMiddleware(middleware);
            startMiddleware(second);

            DistributedPHTree<String> phTree = new DistributedPHTree<>(host, ZK_PORT, String.class);
            phTree.create(dim, depth);
            long[] key = {1, 2};
            phTree.put(key, null);

            assertNull(phTree.get(key));

            phTree.close();
        }
    }

    @Test
    public void testGetKNN() throws Exception {
        int dim = 2;
        int depth = 64;

        String host = "localhost";
        try (TestingServer zkServer = new TestingServer(ZK_PORT);
             Middleware middleware = IndexMiddlewareFactory.newPHTreeMiddleware(7070);
             Middleware second = IndexMiddlewareFactory.newPHTreeMiddleware(7080)
        ) {
            zkServer.start();
            startMiddleware(middleware);
            startMiddleware(second);

            DistributedPHTree<String> phTree = new DistributedPHTree<>(host, ZK_PORT, String.class);
            phTree.create(dim, depth);

            List<long[]> expected = new ArrayList<long[]>() {{
                add(new long[] { 0, 0});
                add(new long[] { 1, 2});
                add(new long[] { -1, -1});
            }};
            List<long[]> inserted = new ArrayList<>();
            inserted.addAll(expected);
            inserted.add(new long[] { -1000, 1000});
            inserted.add(new long[] { -1000, -1000});
            inserted.add(new long[] { 1000, -1000});
            inserted.add(new long[] { 1000, 1000});
            for (long[] key : inserted) {
                phTree.put(key, null);
            }

            List<long[]> nearestNeighbors = phTree.getNearestNeighbors(new long[]{0, 0}, 3);

            expected = MultidimUtil.sort(expected);
            nearestNeighbors = MultidimUtil.sort(nearestNeighbors);

            assertEquals(expected.size(), nearestNeighbors.size());
            for (int i = 0; i < expected.size(); i++) {
                assertArrayEquals(expected.get(i), nearestNeighbors.get(i));
            }
            phTree.close();
        }
    }

    @Test
    public void testPutAndGet2D() throws Exception {
        int dim = 2;
        int depth = 64;
        String host = "localhost";

        try (TestingServer zkServer = new TestingServer(ZK_PORT);
             Middleware middleware = IndexMiddlewareFactory.newPHTreeMiddleware(7070);
             Middleware second = IndexMiddlewareFactory.newPHTreeMiddleware(7080)
        ) {
            zkServer.start();
            startMiddleware(middleware);
            startMiddleware(second);

            DistributedPHTree<String> phTree = new DistributedPHTree<>(host, ZK_PORT, String.class);
            phTree.create(dim, depth);

            long[] key = new long[]{1L, 2L};
            String value = "hello";
            phTree.put(key, value);
            String retrieved = phTree.get(key);
            assertEquals("Wrong value retrieval", value, retrieved);

            key = new long[]{-1L, -2L};
            value = "bye";
            phTree.put(key, value);
            retrieved = phTree.get(key);
            assertEquals("Wrong value retrieval", value, retrieved);
            phTree.close();
        }
    }

    @Test
    public void testDeleteContains() throws Exception {
        int dim = 2;
        int depth = 64;
        String host = "localhost";

        try (TestingServer zkServer = new TestingServer(ZK_PORT);
             Middleware middleware = IndexMiddlewareFactory.newPHTreeMiddleware(7070);
             Middleware second = IndexMiddlewareFactory.newPHTreeMiddleware(7080)) {

            zkServer.start();
            startMiddleware(middleware);
            startMiddleware(second);
            DistributedPHTree<BigInteger> phTree = new DistributedPHTree<>(host, ZK_PORT, BigInteger.class);
            phTree.create(dim, depth);

            long[] key1 = {-1, -1};
            long[] key2 = {1, 1};
            BigInteger value1 = new BigInteger(30, new Random());
            BigInteger value2 = new BigInteger(30, new Random());
            phTree.put(key1, value1);
            phTree.put(key2, value2);

            assertTrue(phTree.contains(key1));
            assertTrue(phTree.contains(key2));

            assertEquals(value1, phTree.remove(key1));
            assertFalse(phTree.contains(key1));
            assertTrue(phTree.contains(key2));

            assertEquals(value2, phTree.remove(key2));
            assertFalse(phTree.contains(key1));
            assertFalse(phTree.contains(key2));

            phTree.close();
        }
    }

    @Test
    public void testPutAndGet3D() throws Exception {
        int dim = 3;
        int depth = 64;
        String host = "localhost";

        try (TestingServer zkServer = new TestingServer(ZK_PORT);
            Middleware middleware = IndexMiddlewareFactory.newPHTreeMiddleware(7070)) {
            zkServer.start();
            startMiddleware(middleware);
            DistributedPHTree<BigInteger> phTree = new DistributedPHTree<>(host, ZK_PORT, BigInteger.class);
            phTree.create(dim, depth);

            int nrEntries = 100000;
            Random random = new Random();

            long[] key;
            BigInteger value;
            for (int i = 0; i < nrEntries; i++) {
                key = new long[]{random.nextLong(), random.nextLong(), random.nextLong()};
                value = new BigInteger(50, random);
                phTree.put(key, value);
                assertEquals("Value does not match with value retrieved from the tree.", value, phTree.get(key));
            }

            phTree.close();
        }
    }

    public void startMiddleware(Middleware middleware) {
        threadPool.execute((Runnable) middleware);
        while (!middleware.isRunning()) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                System.err.format("Failed to sleep while initializing middleware.");
                e.printStackTrace();
            }
        }
    }

    private static long[] k(long... keys) {
        return keys;
    }
}