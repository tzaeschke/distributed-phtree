package ch.ethz.globis.distindex.middleware.api;

import ch.ethz.globis.distindex.middleware.IOHandler;

import java.io.Closeable;

/**
 * Represents a Middleware node.
 */
public interface Middleware extends Closeable, AutoCloseable {

    public void run();

    public boolean isRunning();

    public <K, V> IOHandler<K, V> getHandler();

    public boolean isJoinedAsFree();

    public void setJoinedAsFree(boolean joinedAsFree);
}
