package ch.ethz.globis.distindex.middleware.api;

import java.io.Closeable;

/**
 * Represents a Middleware node.
 */
public interface Middleware extends Closeable, AutoCloseable {

    public void run();

    public boolean isRunning();

    public void setJoinedAsFree(boolean joinedAsFree);

    public void remove();
}
