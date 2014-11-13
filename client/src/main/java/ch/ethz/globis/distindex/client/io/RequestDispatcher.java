package ch.ethz.globis.distindex.client.io;

import ch.ethz.globis.distindex.operation.BaseRequest;
import ch.ethz.globis.distindex.operation.Request;
import ch.ethz.globis.distindex.operation.ResultResponse;
import ch.ethz.globis.distindex.operation.SimpleResponse;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

public interface RequestDispatcher<K, V> extends Closeable, AutoCloseable {

    public ResultResponse<K, V> send(String hostId, Request request);

    public List<ResultResponse<K, V>> send(Collection<String> hostIds, Request request);

    public SimpleResponse sendSimple(String hostId, Request request);

    public List<SimpleResponse> sendSimple(Collection<String> hostIds, Request request);
}