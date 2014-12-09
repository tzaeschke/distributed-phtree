package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.disindex.codec.ByteRequestDecoder;
import ch.ethz.globis.disindex.codec.ByteResponseEncoder;
import ch.ethz.globis.disindex.codec.api.RequestDecoder;
import ch.ethz.globis.disindex.codec.api.ResponseEncoder;
import ch.ethz.globis.disindex.codec.field.MultiLongEncoderDecoder;
import ch.ethz.globis.distindex.middleware.net.BalancingRequestHandler;
import ch.ethz.globis.distindex.middleware.net.IndexMiddleware;
import ch.ethz.globis.distindex.middleware.net.RequestHandler;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import ch.ethz.globis.distindex.orchestration.BSTMapClusterService;

/**
 * Utility class for creating middleware nodes programatically.
 */
public class PhTreeIndexMiddlewareFactory {

    public static IndexMiddleware<long[], byte[]> newPhTree(String host, int port, ClusterService<long[]> clusterService) {
        IndexContext indexContext = new IndexContext(host, port);
        indexContext.setClusterService(clusterService);

        RequestHandler<long[], byte[]> requestHandler = new PhTreeRequestHandler(indexContext);
        BalancingRequestHandler<long[], byte[]> balancingRequestHandler = new PhTreeBalancingRequestHandler(indexContext);
        RequestDecoder<long[], byte[]> requestDecoder = new ByteRequestDecoder<>(new MultiLongEncoderDecoder());
        ResponseEncoder responseEncoder = new ByteResponseEncoder<>(new MultiLongEncoderDecoder());

        IOHandler<long[], byte[]> ioHandler = new IOHandler<>(requestHandler, balancingRequestHandler, requestDecoder, responseEncoder);
        return new IndexMiddleware<>(host, port, clusterService, ioHandler);
    }

    public static IndexMiddleware newPhTree(String host, int port, String zkHost, int zkPort) {
        ClusterService<long[]> clusterService = new BSTMapClusterService(zkHost, zkPort);
        return newPhTree(host, port, clusterService);
    }
}