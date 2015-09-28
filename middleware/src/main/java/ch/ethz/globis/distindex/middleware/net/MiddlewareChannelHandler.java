/*
This file is part of PH-Tree:
A multi-dimensional indexing and storage structure.

Copyright (C) 2011-2015
Eidgenössische Technische Hochschule Zürich (ETH Zurich)
Institute for Information Systems
GlobIS Group
Bogdan Vancea, Tilmann Zaeschke
zaeschke@inf.ethz.ch or zoodb@gmx.de

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
package ch.ethz.globis.distindex.middleware.net;

import ch.ethz.globis.distindex.middleware.IOHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Handles the incoming data on the channel opened by a client.
 *
 * @param <K>
 * @param <V>
 */
public abstract class MiddlewareChannelHandler<K, V> extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(MiddlewareChannelHandler.class);

    private IOHandler<K, V> ioHandler;

    protected MiddlewareChannelHandler(IOHandler<K, V> ioHandler) {
        this.ioHandler = ioHandler;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        String clientHost = ctx.channel().remoteAddress().toString();
        ByteBuffer response = ioHandler.handle(clientHost, buf.nioBuffer());
        ByteBuf nettyBuf = Unpooled.wrappedBuffer(response);
        ByteBuf sizeBuf = Unpooled.copyInt(nettyBuf.readableBytes());

        ctx.write(sizeBuf);
        ctx.write(nettyBuf);
        ctx.flush();
        buf.release();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        String clientHost = ctx.channel().remoteAddress().toString();
        LOG.debug("Client " + clientHost + " disconnected.");
        ioHandler.cleanup(clientHost);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        String clientHost = ctx.channel().remoteAddress().toString();
        LOG.debug("Client " + clientHost + " disconnected.");
        ioHandler.cleanup(clientHost);
        super.exceptionCaught(ctx, cause);
    }
}