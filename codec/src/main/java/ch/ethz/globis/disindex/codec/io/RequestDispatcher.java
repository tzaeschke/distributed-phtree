package ch.ethz.globis.disindex.codec.io;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

import ch.ethz.globis.distindex.operation.request.Request;
import ch.ethz.globis.distindex.operation.response.Response;

public interface RequestDispatcher<K, V> extends Closeable, AutoCloseable {

    public <R extends Response> R send(String hostId, Request request, Class<R> clazz);

    public <R extends Response> List<R> send(Collection<String> hostIds, Request request, Class<R> clazz);
}