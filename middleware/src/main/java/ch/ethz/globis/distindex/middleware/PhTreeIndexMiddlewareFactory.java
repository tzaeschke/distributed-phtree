package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.disindex.codec.ByteRequestDecoder;
import ch.ethz.globis.disindex.codec.ByteResponseEncoder;
import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.distindex.middleware.IOHandler;
import ch.ethz.globis.distindex.middleware.IndexContext;
import ch.ethz.globis.distindex.middleware.PhTreeRequestHandler;
import ch.ethz.globis.distindex.middleware.net.BalancingRequestHandler;
import ch.ethz.globis.distindex.middleware.net.IndexMiddleware;
import ch.ethz.globis.distindex.middleware.net.RequestHandler;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;

/**
 * Utility class for creating middleware nodes programatically.
 */
public class PhTreeIndexMiddlewareFactory {

    public static <K, V> IndexMiddleware newMiddleware(String host, int port, String zkHost, int zkPort, IOHandler<K, V> ioHandler) {
        ClusterService<long[]> clusterService = new ZKClusterService(zkHost, zkPort);
        return new IndexMiddleware<>(host, port, clusterService, ioHandler);
    }

    public static IndexMiddleware<long[], byte[]> newPhTree(String host, int port, ClusterService<long[]> clusterService) {
        IndexContext indexContext = new IndexContext();
        RequestHandler<long[], byte[]> requestHandler = new PhTreeRequestHandler(indexContext);
        BalancingRequestHandler<long[], byte[]> balancingRequestHandler = new PhTreeBalancingRequestHandler(indexContext);
        RequestDecoder<long[], byte[]> requestDecoder = new ByteRequestDecoder<>(new MultiLongEncoderDecoder());
        ResponseEncoder<long[], byte[]> responseEncoder = new ByteResponseEncoder<>(new MultiLongEncoderDecoder());

        IOHandler<long[], byte[]> ioHandler = new IOHandler<>(requestHandler, balancingRequestHandler, requestDecoder, responseEncoder);
        return new IndexMiddleware<>(host, port, clusterService, ioHandler);
    }

    public static IndexMiddleware newPhTree(String host, int port, String zkHost, int zkPort) {
        IndexContext indexContext = new IndexContext();
        RequestHandler<long[], byte[]> requestHandler = new PhTreeRequestHandler(indexContext);
        BalancingRequestHandler<long[], byte[]> balancingRequestHandler = new PhTreeBalancingRequestHandler(indexContext);
        RequestDecoder<long[], byte[]> requestDecoder = new ByteRequestDecoder<>(new MultiLongEncoderDecoder());
        ResponseEncoder<long[], byte[]> responseEncoder = new ByteResponseEncoder<>(new MultiLongEncoderDecoder());

        IOHandler<long[], byte[]> ioHandler = new IOHandler<>(requestHandler, balancingRequestHandler, requestDecoder, responseEncoder);
        return newMiddleware(host, port, zkHost, zkPort, ioHandler);
    }

    public static IndexMiddleware newLocalPhTree(int port) {
        return newPhTree("localhost", port, "localhost", 2181);
    }
}