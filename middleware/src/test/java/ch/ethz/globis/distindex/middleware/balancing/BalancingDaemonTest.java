package ch.ethz.globis.distindex.middleware.balancing;

import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BalancingDaemonTest {

    @Test
    public void testBalancingCheck() {
        BalancingDaemon daemon = new BalancingDaemon(null, null, 1000L);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.execute(daemon);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
           daemon.close();
           pool.shutdown();
        }

    }
}