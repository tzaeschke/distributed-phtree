package ch.ethz.globis.distindex.middleware.balancing;

import ch.ethz.globis.distindex.middleware.IndexContext;
import ch.ethz.globis.distindex.middleware.PhTreeRequestHandler;
import ch.ethz.globis.pht.PhTreeV;

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
            PhTreeV<byte[]> tree = indexContext.getTree();
            if (tree.size() > PhTreeRequestHandler.THRESHOLD) {
                balancingStrategy.balance();
            }
        }
    }
}
