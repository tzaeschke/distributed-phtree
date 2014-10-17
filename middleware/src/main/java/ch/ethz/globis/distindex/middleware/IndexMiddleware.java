package ch.ethz.globis.distindex.middleware;

import ch.ethz.globis.distindex.middleware.api.Middleware;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.util.Properties;

/**
 * Distributed Index Server implemented using Netty.io
 */
public class IndexMiddleware<V> implements Middleware, Runnable {

    private int port;

    private boolean isRunning = false;

    private Properties properties;

    /** The thread pool dealing with receiving data over network */
    private EventLoopGroup bossGroup;

    /** The thread pool dealing with handling the data received */
    private EventLoopGroup workerGroup;

    public IndexMiddleware(int port, Properties properties) {
        this.port = port;
        this.properties = properties;
    }

    @Override
    public void run() {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = initServerBootstrap(properties);

            ChannelFuture f = b.bind(port).sync();

            isRunning = true;

            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            System.err.format("An error occurred while operating on the channel");
            e.printStackTrace();
        } finally {
            closeEventLoops();
        }
    }

    @Override
    public void shutdown() {
        if (bossGroup == null || workerGroup == null) {
            throw new IllegalStateException("The thread pools are not properly initialized");
        }
        closeEventLoops();
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
}