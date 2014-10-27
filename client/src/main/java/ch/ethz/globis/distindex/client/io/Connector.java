package ch.ethz.globis.distindex.client.io;

import ch.ethz.globis.distindex.operation.Request;
import ch.ethz.globis.distindex.operation.Response;

import java.util.List;

public interface Connector<K, V> {

    public Response<K, V> send(String hostId, Request request);

    public List<Response<K, V>> send(List<String> hostIds, Request request);
}