package ch.ethz.globis.distindex.mapping.zorder;

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
}
