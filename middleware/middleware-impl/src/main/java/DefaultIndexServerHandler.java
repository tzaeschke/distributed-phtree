import ch.ethz.globis.disindex.codec.RequestCode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class DefaultIndexServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //ToDo send the data structure containing intervals
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        int messageCode = getMessageCode(buf);

        ByteBuf response;
        switch (messageCode) {
            case RequestCode.GET:
                response = handleGetRequest(buf);
                break;
            case RequestCode.GET_KNN:
                response = handleGetKNNRequest(buf);
                break;
            case RequestCode.GET_RANGE:
                response = handleGetRangeRequest(buf);
                break;
            case RequestCode.PUT:
                response = handlePutRequest(buf);
                break;
            default:
                response = handleErroneousRequest(buf);
        }
        ctx.writeAndFlush(response);
    }

    private ByteBuf handleGetRequest(ByteBuf buf) {
        return emptyBuffer();
    }

    private ByteBuf handleGetRangeRequest(ByteBuf buf) {
        return emptyBuffer();
    }

    private ByteBuf handleGetKNNRequest(ByteBuf buf) {
        return emptyBuffer();
    }

    private ByteBuf handlePutRequest(ByteBuf buf) {
        return emptyBuffer();
    }

    private ByteBuf handleErroneousRequest(ByteBuf buf) {
        return emptyBuffer();
    }

    private ByteBuf emptyBuffer() {
        ByteBufAllocator alloc = PooledByteBufAllocator.DEFAULT;
        return alloc.buffer();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.err.format("Exception!");
        cause.printStackTrace();
    }

    private int getMessageCode(ByteBuf buf) {
        return 0;
    }
}
