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
