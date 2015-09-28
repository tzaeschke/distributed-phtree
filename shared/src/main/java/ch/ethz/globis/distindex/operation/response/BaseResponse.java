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
package ch.ethz.globis.distindex.operation.response;

/**
 * Base request, when only an acknowledgement with status is needed.
 */
public class BaseResponse implements Response {

    private byte type;
    private byte opCode;
    private int requestId;
    private byte status;

    public BaseResponse() { }

    public BaseResponse(byte opCode, int requestId, byte status) {
        this.type = ResponseCode.BASE;
        this.opCode = opCode;
        this.requestId = requestId;
        this.status = status;
    }

    public BaseResponse(byte type, byte opCode, int requestId, byte status) {
        this.type = type;
        this.opCode = opCode;
        this.requestId = requestId;
        this.status = status;
    }

    @Override
    public byte getOpCode() {
        return opCode;
    }

    @Override
    public byte getStatus() {
        return status;
    }

    @Override
    public int getRequestId() {
        return requestId;
    }

    @Override
    public byte getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BaseResponse)) return false;

        BaseResponse that = (BaseResponse) o;

        if (opCode != that.opCode) return false;
        if (requestId != that.requestId) return false;
        if (status != that.status) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) opCode;
        result = 31 * result + requestId;
        result = 31 * result + (int) status;
        return result;
    }
}