import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

/**
 * Distributed Index Server implemented using Netty.io
 */
public class DefaultIndexServer implements DistributedIndexServer {

    private int port;

    /** The thread pool dealing with receiving data over network */
    private EventLoopGroup bossGroup;

    /** The thread pool dealing with handling the data received */
    private EventLoopGroup workerGroup;

    public DefaultIndexServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = initServerBoostrap();

            ChannelFuture f = b.bind(port).sync();

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

    private ServerBootstrap initServerBoostrap() {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new DefaultServerChannelInitializer());
        return b;
    }

    private void closeEventLoops() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
