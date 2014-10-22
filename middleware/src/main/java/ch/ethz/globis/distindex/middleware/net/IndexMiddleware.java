package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.ClusterService;
import ch.ethz.globis.distindex.middleware.api.Middleware;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * Distributed Index Server implemented using Netty.io
 */
public class IndexMiddleware<V> implements Middleware, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexMiddleware.class);

    /** The host on which the middleware is running */
    private String host;

    /** The port on which the middleware is running */
    private int port;

    /** A flag determining if the middleware is running or not.*/
    private boolean isRunning = false;

    /** The properties used to initialize the middleware. */
    private Properties properties;

    /** The thread pool dealing with receiving data over network */
    private EventLoopGroup bossGroup;

    /** The thread pool dealing with handling the data received */
    private EventLoopGroup workerGroup;

    /** The cluster service used to notify */
    private ClusterService clusterService;

    public IndexMiddleware(String host, int port, ClusterService clusterService, Properties properties) {
        this.clusterService = clusterService;
        this.port = port;
        this.properties = properties;
        this.host = host;
    }

    @Override
    public void run() {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        try {
            //initialize the server channels
            ServerBootstrap b = initServerBootstrap(properties);
            ChannelFuture f = b.bind(port).sync();

            //register as a viable host to the cluster service
            clusterService.connect();
            clusterService.registerHost(getHostId());
            isRunning = true;

            f.channel().closeFuture().sync();
        } catch (InterruptedException ie) {
            LOG.error("An error occurred while operating on the channel.", ie);
            ie.printStackTrace();
        } finally {
            closeEventLoops();
            isRunning = false;
            //disconnect the cluster service
            clusterService.disconnect();
        }
    }

    @Override
    public void shutdown() {
        if (bossGroup == null || workerGroup == null) {
            throw new IllegalStateException("The thread pools are not properly initialized");
        }
        clusterService.disconnect();
        closeEventLoops();
        LOG.info("Shutting down middleware {} {} ", host, port);
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    private ServerBootstrap initServerBootstrap(Properties properties) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new MiddlewareChannelInitializer<V>(properties));
        return b;
    }

    private void closeEventLoops() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    private String getHostId() {
        return host + ":" + port;
    }
}