package ch.ethz.globis.distindex.orchestration;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperConnectionWatcher implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperConnectionWatcher.class);

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.debug("Received message from Zookeeper {}", watchedEvent);
    }
}
