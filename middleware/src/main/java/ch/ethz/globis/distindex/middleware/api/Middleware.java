package ch.ethz.globis.distindex.middleware.api;

public interface Middleware {

    public void run();

    public void shutdown();

    public boolean isRunning();
}
