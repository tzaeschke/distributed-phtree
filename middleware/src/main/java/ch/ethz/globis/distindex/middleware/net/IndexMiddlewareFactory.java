package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.ClusterService;
import ch.ethz.globis.distindex.middleware.config.IndexProperties;
import ch.ethz.globis.distindex.orchestration.ZKClusterService;

import java.util.Properties;

/**
 * Utility class for creating middleware nodes programatically.
 */
public class IndexMiddlewareFactory {

    private static final String DEFAULT_ZK_CONNECTION = "localhost:2181";

    @Deprecated
    public static IndexMiddleware newPHTreeMiddleware(int port, int dim, int depth) {
        Properties properties = new Properties();
        properties.setProperty(IndexProperties.INDEX_DIM, String.valueOf(dim));
        properties.setProperty(IndexProperties.INDEX_DEPTH, String.valueOf(depth));

        ClusterService clusterService = new ZKClusterService(DEFAULT_ZK_CONNECTION);
        return new IndexMiddleware("localhost", port, clusterService, properties);
    }

    public static IndexMiddleware newPHTreeMiddleware(int port) {
        Properties properties = new Properties();

        ClusterService clusterService = new ZKClusterService(DEFAULT_ZK_CONNECTION);
        return new IndexMiddleware("localhost", port, clusterService, properties);
    }
}