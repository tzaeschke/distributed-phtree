package ch.ethz.globis.distindex.client.io;

import ch.ethz.globis.distindex.operation.request.Request;
import ch.ethz.globis.distindex.operation.response.ResultResponse;
import ch.ethz.globis.distindex.operation.response.SimpleResponse;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

public interface RequestDispatcher<K, V> extends Closeable, AutoCloseable {

    public ResultResponse<K, V> send(String hostId, Request request);

    public List<ResultResponse<K, V>> send(Collection<String> hostIds, Request request);

    public SimpleResponse sendSimple(String hostId, Request request);

    public List<SimpleResponse> sendSimple(Collection<String> hostIds, Request request);
}