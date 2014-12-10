package ch.ethz.globis.disindex.codec.io;

import ch.ethz.globis.distindex.operation.request.Request;
import ch.ethz.globis.distindex.operation.response.Response;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.distindex.operation.response.SimpleResponse;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

public interface RequestDispatcher<K, V> extends Closeable, AutoCloseable {

    public <R extends Response> R send(String hostId, Request request, Class<R> clazz);

    public <R extends Response> List<R> send(Collection<String> hostIds, Request request, Class<R> clazz);
}