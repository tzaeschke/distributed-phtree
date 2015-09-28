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
package ch.ethz.globis.distindex.mapping.zorder;

import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ZOrderServiceTest {

    @Test
    public void testEnvelope_PositiveRange() {
        int depth = 3;
        long[] start = { 1L, 1L};
        long[] end = { 3L, 3L};
        ZOrderService service = new ZOrderService(depth);
        Set<HBox> boxes = service.regionEnvelope(start, end);
        Set<HBox> expected = new HashSet<HBox>() {{
            add(new HBox("0001"));
            add(new HBox("0010"));
            add(new HBox("00110"));
            add(new HBox("001110"));
        }};
        assertEquals(expected, boxes);
    }

    @Test
    public void testLowerGroupConstruction_Small_0() {
        int dim = 2;
        int depth = 1;
        ZOrderService service = new ZOrderService(depth);
        ZAddress beta = new ZAddress("00", dim);
        HBox spaceBox = new HBox();
        Set<HBox> quads = service.lowerHalfEnvelope(spaceBox, beta, dim);
        Set<HBox> expected = new HashSet<>();
        assertEquals(expected, quads);
    }

    @Test
    public void testLowerGroupConstruction_Small_1() {
        int dim = 2;
        int depth = 1;
        ZOrderService service = new ZOrderService(depth);
        ZAddress beta = new ZAddress("01", dim);
        HBox spaceBox = new HBox();
        Set<HBox> quads = service.lowerHalfEnvelope(spaceBox, beta, dim);
        Set<HBox> expected = new HashSet<HBox>() {{
            add(new HBox("00"));
        }};
        assertEquals(expected, quads);
    }

    @Test
    public void testLowerGroupConstruction_Small_2() {
        int dim = 2;
        int depth = 1;
        ZOrderService service = new ZOrderService(depth);
        ZAddress beta = new ZAddress("10", dim);
        HBox spaceBox = new HBox();
        Set<HBox> quads = service.lowerHalfEnvelope(spaceBox, beta, dim);
        Set<HBox> expected = new HashSet<HBox>() {{
            add(new HBox("0"));
        }};
        assertEquals(expected, quads);
    }

    @Test
    public void testLowerGroupConstruction_Small_3() {
        int dim = 2;
        int depth = 1;
        ZOrderService service = new ZOrderService(depth);
        ZAddress beta = new ZAddress("11", dim);
        HBox spaceBox = new HBox();
        Set<HBox> quads = service.lowerHalfEnvelope(spaceBox, beta, dim);
        Set<HBox> expected = new HashSet<HBox>() {{
            add(new HBox("10"));
            add(new HBox("0"));
        }};
        assertEquals(expected, quads);
    }

    /**
     * Visualization
     *
     * +-----------+-----------+-----------------------+
     * |           |           |                       |
     * |           |           |                       |
     * |           |           |                       |
     * |   0000    |    0010   |                       |
     * |           |           |                       |
     * |           |           |                       |
     * |           |           |                       |
     * +--------------+--+-----+          10           |
     * |           |  |xx|     |                       |
     * |           +-----+     |                       |
     * |           |  |  |     |                       |
     * |   0001    +--+--------+                       |
     * |           |     |     |                       |
     * |           |     |     |                       |
     * |           |     |     |                       |
     * +-----------+-----+-----------------------------+
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |           01          |          11           |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * +-----------------------+-----------------------+
     *
     */
    @Test
    public void testLowerGroupConstruction_Large() {
        int dim = 2;
        int depth = 4;
        ZOrderService service = new ZOrderService(depth);
        ZAddress beta = new ZAddress("00110010", dim);
        HBox spaceBox = new HBox();
        Set<HBox> quads = service.lowerHalfEnvelope(spaceBox, beta, dim);
        Set<HBox> expected = new HashSet<HBox>() {{
            add(new HBox("000"));
            add(new HBox("0010"));
            add(new HBox("0011000"));
        }};
        assertEquals(expected, quads);
    }

    @Test
    public void testUpperGroupConstruction_Small_0() {
        int dim = 2;
        int depth = 1;
        ZOrderService service = new ZOrderService(depth);
        ZAddress beta = new ZAddress("00", dim);
        HBox spaceBox = new HBox();
        Set<HBox> quads = service.upperHalfEnvelope(spaceBox, beta, dim);
        Set<HBox> expected = new HashSet<HBox>() {{
            add(new HBox("1"));
            add(new HBox("01"));
        }};
        assertEquals(expected, quads);
    }

    @Test
    public void testUpperGroupConstruction_Small_1() {
        int dim = 2;
        int depth = 1;
        ZOrderService service = new ZOrderService(depth);
        ZAddress beta = new ZAddress("01", dim);
        HBox spaceBox = new HBox();
        Set<HBox> quads = service.upperHalfEnvelope(spaceBox, beta, dim);
        Set<HBox> expected = new HashSet<HBox>() {{
            add(new HBox("1"));
        }};
        assertEquals(expected, quads);
    }

    @Test
    public void testUpperGroupConstruction_Small_2() {
        int dim = 2;
        int depth = 1;
        ZOrderService service = new ZOrderService(depth);
        ZAddress beta = new ZAddress("10", dim);
        HBox spaceBox = new HBox();
        Set<HBox> quads = service.upperHalfEnvelope(spaceBox, beta, dim);
        Set<HBox> expected = new HashSet<HBox>() {{
            add(new HBox("11"));
        }};
        assertEquals(expected, quads);
    }

    @Test
    public void testUpperGroupConstruction_Small_3() {
        int dim = 2;
        int depth = 1;
        ZOrderService service = new ZOrderService(depth);
        ZAddress beta = new ZAddress("11", dim);
        HBox spaceBox = new HBox();
        Set<HBox> quads = service.upperHalfEnvelope(spaceBox, beta, dim);
        Set<HBox> expected = new HashSet<>();
        assertEquals(expected, quads);
    }

    /**
     * Visualization
     *
     * +-----------+-----------+-----------------------+
     * |           |           |                       |
     * |           |           |                       |
     * |           |           |                       |
     * |   0000    |    0010   |                       |
     * |           |           |                       |
     * |           |           |                       |
     * |           |           |                       |
     * +--------------+--+-----+          10           |
     * |           |  |xx|     |                       |
     * |           +-----+     |                       |
     * |           |  |  |     |                       |
     * |   0001    +--+--------+                       |
     * |           |     |     |                       |
     * |           |     |     |                       |
     * |           |     |     |                       |
     * +-----------+-----+-----------------------------+
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |           01          |          11           |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * |                       |                       |
     * +-----------------------+-----------------------+
     *
     */
    @Test
    public void testUpperGroupConstruction_Large() {
        int dim = 2;
        int depth = 4;
        ZOrderService service = new ZOrderService(depth);
        ZAddress beta = new ZAddress("00110010", dim);
        HBox spaceBox = new HBox();
        Set<HBox> quads = service.upperHalfEnvelope(spaceBox, beta, dim);
        Set<HBox> expected = new HashSet<HBox>() {{
            add(new HBox("1"));
            add(new HBox("01"));
            add(new HBox("00111"));
            add(new HBox("001101"));
            add(new HBox("00110011"));

        }};
        assertEquals(expected, quads);
    }

    @Test
    public void testRegionsBetweenConstruction_4D() {
        int dim = 4;
        int depth = 64;
        ZOrderService service = new ZOrderService(depth);
        Set<HBox> regions = service.getRegionsBetweenWithEnvelopes("0000", "1111", dim);
        System.out.println(regions);
    }

    @Test
    public void testRegionsBetweenConstruction_3D() {
        int dim = 3;
        int depth = 64;
        ZOrderService service = new ZOrderService(depth);
        Set<HBox> regions = service.getRegionsBetweenWithEnvelopes("000", "111", dim);
        System.out.println(regions);

        regions = service.getRegionsBetweenWithEnvelopes("000", "011", dim);
        System.out.println(regions);
    }

    @Test
    public void testRegionsBetweenConstruction_2D() {
        int dim = 2;
        int depth = 64;
        ZOrderService service = new ZOrderService(depth);
        Set<HBox> regions = service.getRegionsBetweenWithEnvelopes("00", "11", dim);
        System.out.println(regions);

        regions = service.getRegionsBetweenWithEnvelopes("01", "11", dim);
        System.out.println(regions);

        regions = service.getRegionsBetweenWithEnvelopes(new HBox("00"), "01", "11", dim);
        System.out.println(regions);


    }
}
