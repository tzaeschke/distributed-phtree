package ch.ethz.globis.distindex.middleware.balancing;

import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BalancingDaemonTest {

    @Test
    public void testBalancingCheck() {
        BalancingDaemon daemon = new BalancingDaemon(null, null, 10L);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.execute(daemon);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
           daemon.close();
           pool.shutdown();
        }

    }
}