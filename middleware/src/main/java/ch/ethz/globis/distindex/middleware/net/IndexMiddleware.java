package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.middleware.IOHandler;
import ch.ethz.globis.distindex.middleware.IndexContext;
import ch.ethz.globis.distindex.middleware.api.Middleware;
import ch.ethz.globis.distindex.middleware.balancing.BalancingDaemon;
import ch.ethz.globis.distindex.orchestration.ClusterService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed Index Server implemented using Netty.io
 */
public class IndexMiddleware<K, V>  implements Middleware, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexMiddleware.class);

    /** Wether the host joins as free or not. */
    private boolean joinedAsFree = false;

    /** The host on which the middleware is running */
    private String host;

    /** The port on which the middleware is running */
    private int port;

    /** A flag determining if the middleware is running or not.*/
    private boolean isRunning = false;

    /** The properties used to initialize the middleware. */
    private IOHandler<K, V> handler;

    /** The thread pool dealing with receiving data over network */
    private EventLoopGroup bossGroup;

    /** The thread pool dealing with handling the data received */
    private EventLoopGroup workerGroup;

    /** The cluster service used to notify */
    private ClusterService clusterService;

    private IndexContext indexContext;

    private BalancingDaemon balancingDaemon;

    public IndexMiddleware(IndexContext indexContext, ClusterService clusterService, IOHandler<K, V> handler,
                           BalancingDaemon balancingDaemon) {
        this.clusterService = clusterService;
        this.indexContext = indexContext;
        this.port = indexContext.getPort();
        this.host = indexContext.getHost();
        this.handler = handler;
        this.balancingDaemon = balancingDaemon;
    }

    @Override
    public void run() {

        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        balancingDaemon.run();
        try {
            //initialize the server channels
            ServerBootstrap b = initServerBootstrap(handler);
            ChannelFuture f = b.bind(port).sync();

            //register as a viable host to the cluster service
            clusterService.connect();
            if (joinedAsFree) {
                clusterService.registerFreeHost(getHostId());
            } else {
                clusterService.registerHost(getHostId());
            }
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
    public void close() {
        while (indexContext.isBalancing()) {
            try {
                Thread.sleep(1L);
            } catch (InterruptedException e) {
                LOG.error("Error ");
            }
        }
        if (bossGroup == null || workerGroup == null) {
            throw new IllegalStateException("The thread pools are not properly initialized");
        }
        clusterService.disconnect();
        balancingDaemon.close();
        closeEventLoops();
        LOG.info("Shutting down middleware {} {} ", host, port);
    }

    @Override
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public void remove() {
        balancingDaemon.balanceAndRemove();
        close();
    }

    private <K, V> ServerBootstrap initServerBootstrap(final IOHandler<K, V> handler) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new MiddlewareMessageDecoder(), new MiddlewareChannelHandler<K, V>(handler) {});
                    }
                });
        return b;
    }

    private void closeEventLoops() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    private String getHostId() {
        return host + ":" + port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isJoinedAsFree() {
        return joinedAsFree;
    }

    public void setJoinedAsFree(boolean joinedAsFree) {
        this.joinedAsFree = joinedAsFree;
    }

}