package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.disindex.codec.ByteRequestDecoder;
import ch.ethz.globis.disindex.codec.ByteResponseEncoder;
import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.distindex.middleware.IOHandler;
import ch.ethz.globis.distindex.middleware.PhTreeRequestHandler;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;

/**
 * Utility class for creating middleware nodes programatically.
 */
public class IndexMiddlewareFactory {

    public static IndexMiddleware newPHTreeMiddleware(int port) {
        return newPhTree("localhost", port, "localhost", 2181);
    }

    public static <K, V> IndexMiddleware newMiddleware(String host, int port, String zkHost, int zkPort, IOHandler<K, V> ioHandler) {
        ClusterService clusterService = new ZKClusterService(zkHost, zkPort);
        return new IndexMiddleware<>(host, port, clusterService, ioHandler);
    }

    public static IndexMiddleware newPhTree(String host, int port, String zkHost, int zkPort) {
        RequestHandler<long[], byte[]> requestHandler = new PhTreeRequestHandler();
        RequestDecoder<long[], byte[]> requestDecoder = new ByteRequestDecoder<>(new MultiLongEncoderDecoder());
        ResponseEncoder<long[], byte[]> responseEncoder = new ByteResponseEncoder<>(new MultiLongEncoderDecoder());

        IOHandler<long[], byte[]> ioHandler = new IOHandler<>(requestHandler, requestDecoder, responseEncoder);
        return newMiddleware(host, port, zkHost, zkPort, ioHandler);
    }

    public static IndexMiddleware newLocalPhTree(int port) {
        return newPhTree("localhost", port, "localhost", 2181);
    }
}