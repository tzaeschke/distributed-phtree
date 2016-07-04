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
package ch.ethz.globis.distindex.operation;

public class OpCode {

    public static final byte CREATE_INDEX = 1;
    public static final byte PUT = 10;
    public static final byte DELETE = 11;
    public static final byte GET = 20;
    public static final byte GET_RANGE = 21;
    public static final byte GET_KNN = 22;
    public static final byte GET_BATCH = 23;
    public static final byte GET_SIZE = 24;
    public static final byte GET_DIM = 25;
    public static final byte GET_DEPTH = 26;
    public static final byte CLOSE_ITERATOR = 27;
    public static final byte CONTAINS = 28;

    public static final byte BALANCE_INIT = 30;
    public static final byte BALANCE_PUT = 31;
    public static final byte BALANCE_COMMIT = 32;
    public static final byte BALANCE_ROLLBACK = 33;

    public static final byte STATS = 41;
    public static final byte TO_STRING = 42;
 
    public static final byte UPDATE_KEY = 46;
    public static final byte GET_RANGE_FILTER = 47;
}