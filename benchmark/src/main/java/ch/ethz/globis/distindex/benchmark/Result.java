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
package ch.ethz.globis.distindex.benchmark;

public class Result {

    private final long start;
    private final long end;
    private final long nrOperations;

    private final double avgResponseTime;

    public Result(long start, long end, long nrOperations, double avgResponseTime) {
        this.start = start;
        this.end = end;
        this.nrOperations = nrOperations;
        this.avgResponseTime = avgResponseTime;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public long getNrOperations() {
        return nrOperations;
    }

    public double getAvgResponseTime() {
        return avgResponseTime;
    }

    public double getThroughput() {
        double duration = (end - start) / 1000.0;
        return nrOperations / duration;
    }

    @Override
    public String toString() {
        double duration = (end - start) / 1000.0;
        double tp = nrOperations / duration;
        String pattern = "%10.5f\t%10.5f";
        return String.format(pattern, tp, avgResponseTime);
    }
}