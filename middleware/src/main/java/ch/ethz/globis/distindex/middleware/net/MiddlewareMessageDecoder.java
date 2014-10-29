package ch.ethz.globis.distindex.middleware.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * Buffers the byte chunks received from the client until a full message was received.
 *
 * When this happens, the ByteBuf containing that message is send to the channel handler.
 */
public class MiddlewareMessageDecoder extends ByteToMessageDecoder{

    private int bytesToRead = -1;

    /**
     * Checks whether the bytes accumulated in the in Buffer constitute a full message sent
     * from the client. If that is the case, the message is copied to the out list and the
     * next handler is notified.
     *
     * @param ctx                           The Netty context associated with the channel.
     * @param in                            A Netty managed buffer that holds the accumulated received chunks.
     * @param out                           The list of objects to be passed to the next handler.
     * @throws Exception
     */
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
