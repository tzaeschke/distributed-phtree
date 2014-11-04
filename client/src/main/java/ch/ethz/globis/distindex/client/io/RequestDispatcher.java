package ch.ethz.globis.distindex.client.io;

import ch.ethz.globis.distindex.operation.BaseRequest;
import ch.ethz.globis.distindex.operation.ResultResponse;
import ch.ethz.globis.distindex.operation.SimpleResponse;

import java.io.Closeable;
import java.util.List;

public interface RequestDispatcher<K, V> extends Closeable, AutoCloseable {

    public ResultResponse<K, V> send(String hostId, BaseRequest request);

    public List<ResultResponse<K, V>> send(List<String> hostIds, BaseRequest request);

    public SimpleResponse sendSimple(String hostId, BaseRequest request);

    public List<SimpleResponse> sendSimple(List<String> hostIds, BaseRequest request);
}