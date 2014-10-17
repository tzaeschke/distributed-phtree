package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.distindex.middleware.config.IndexProperties;

import java.util.Properties;

public class IndexMiddlewareFactory {

    public static <V> IndexMiddleware<V> newPHTreeMiddleware(int port, int dim, int depth, Class<V> valueClass) {
        Properties properties = new Properties();
        properties.setProperty(IndexProperties.INDEX_DIM, String.valueOf(dim));
        properties.setProperty(IndexProperties.INDEX_DEPTH, String.valueOf(depth));
        properties.setProperty(IndexProperties.INDEX_VALUE_CLASS, valueClass.getName());

        return new IndexMiddleware<V>(port, properties);
    }
}
