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

    public static <V> IndexMiddleware<V> newPHTreeMiddleware(int port, int dim, int depth, Class<V> valueClass) {
        Properties properties = new Properties();
        properties.setProperty(IndexProperties.INDEX_DIM, String.valueOf(dim));
        properties.setProperty(IndexProperties.INDEX_DEPTH, String.valueOf(depth));
        properties.setProperty(IndexProperties.INDEX_VALUE_CLASS, valueClass.getName());

        ClusterService clusterService = new ZKClusterService(DEFAULT_ZK_CONNECTION);
        return new IndexMiddleware<>("localhost", port, clusterService, properties);
    }
}
