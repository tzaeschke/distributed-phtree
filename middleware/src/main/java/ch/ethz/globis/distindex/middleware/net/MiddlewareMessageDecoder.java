package ch.ethz.globis.distindex.middleware.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class MiddlewareMessageDecoder extends ByteToMessageDecoder{

    private int bytesToRead = -1;
    private ByteBuf internalBuffer;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 4) {
            //need to know exactly how many bytes to read
            return;
        }
        if (bytesToRead == -1) {
            bytesToRead = in.readInt();
        }

        if (in.readableBytes() == bytesToRead) {
            out.add(in.readBytes(in.readableBytes()));
            bytesToRead = -1;
        }
    }
}
