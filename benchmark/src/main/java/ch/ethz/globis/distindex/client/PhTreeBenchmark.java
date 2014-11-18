package ch.ethz.globis.distindex.client;

import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.distindex.client.pht.PHTreeIndexProxy;
import ch.ethz.globis.distindex.client.pht.ZKPHFactory;
import ch.ethz.globis.distindex.middleware.api.Middleware;
import ch.ethz.globis.distindex.middleware.net.IndexMiddlewareFactory;

import ch.ethz.globis.distindex.middleware.util.MiddlewareUtil;
import org.apache.curator.test.TestingServer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class PhTreeBenchmark {

    private static Logger LOG = LoggerFactory.getLogger(PhTreeBenchmark.class);

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        private static PHTreeIndexProxy<Object> indexProxy;

        private static final String ZK_HOST = "localhost";
        private static final int ZK_PORT = 2181;
        private static final int S_BASE_PORT = 7070;
        static final int NUMBER_OF_SERVERS = 4;

        private static ExecutorService threadPool;
        private static TestingServer zkServer;
        private static List<Middleware> middlewares = new ArrayList<Middleware>();

        @Setup
        public void init() {
            setupCluster();
            indexProxy = setupIndex();
            insert();
        }

        @TearDown
        public void close() throws IOException {
            for (Middleware middleware : middlewares) {
                middleware.close();
            }
            threadPool.shutdownNow();
            zkServer.close();
        }

        private static PHTreeIndexProxy<Object> setupIndex() {
            PHFactory factory = new ZKPHFactory(ZK_HOST, ZK_PORT);
            PHTreeIndexProxy<Object> proxy = factory.createProxy(2, 64);
            return proxy;
        }

        private static void insert() {
            Random random = new Random();
            for (int i = 0; i < 100; i++) {
                long[] key = { random.nextLong(), random.nextLong() };
                indexProxy.put(key, new BigInteger(64, random));
            }
        }

        public static void setupCluster() {
            try {
                threadPool = Executors.newFixedThreadPool(NUMBER_OF_SERVERS * 2);
                zkServer = new TestingServer(ZK_PORT);
                zkServer.start();

                for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
                    Middleware current = IndexMiddlewareFactory.newPhTree(ZK_HOST, S_BASE_PORT + i * 10, ZK_HOST, ZK_PORT);
                    MiddlewareUtil.startMiddleware(threadPool, current);
                    middlewares.add(current);
                }
            } catch (IOException e) {
                LOG.error("Failed to create the new testing utility.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.NANOSECONDS)
    @Measurement(iterations = 20, time = 100, timeUnit = TimeUnit.NANOSECONDS)
    public Object benchmarkNonRange(BenchmarkState state) {
        state.indexProxy.setRangeKNN(false);
        return state.indexProxy.getNearestNeighbors(randomKey(), 10);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.NANOSECONDS)
    @Measurement(iterations = 20, time = 100, timeUnit = TimeUnit.NANOSECONDS)
    public Object benchmarkRange(BenchmarkState state) {
        state.indexProxy.setRangeKNN(true);
        return state.indexProxy.getNearestNeighbors(randomKey(), 10);
    }

    private static long[] randomKey() {
        Random random = new Random();
        return new long[] { random.nextLong(), random.nextLong() };
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + PhTreeBenchmark.class.getSimpleName() + ".*")
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}