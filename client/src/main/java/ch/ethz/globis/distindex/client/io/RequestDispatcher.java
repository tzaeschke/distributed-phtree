package ch.ethz.globis.distindex.client.io;

import ch.ethz.globis.distindex.operation.Request;
import ch.ethz.globis.distindex.operation.Response;
import ch.ethz.globis.distindex.operation.SimpleResponse;

import java.io.Closeable;
import java.util.List;

public interface RequestDispatcher<K, V> extends Closeable, AutoCloseable {

    public Response<K, V> send(String hostId, Request request);

    public List<Response<K, V>> send(List<String> hostIds, Request request);

    public SimpleResponse sendSimple(String hostId, Request request);

    public List<SimpleResponse> sendSimple(List<String> hostIds, Request request);
}