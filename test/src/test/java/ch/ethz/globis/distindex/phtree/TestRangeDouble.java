package ch.ethz.globis.distindex.phtree;

import ch.ethz.globis.distindex.BaseParameterizedTest;
import ch.ethz.globis.distindex.client.pht.ZKPHFactory;
import ch.ethz.globis.pht.PhTreeRangeD;
import ch.ethz.globis.pht.PhTreeRangeD.PHREntry;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * //ToDO check what happens if number of hosts > 4
 */
public class TestRangeDouble extends BaseParameterizedTest {

    private PhTreeRangeD pht;
    private PHREntry e1, e2, e3;
    private ZKPHFactory factory;

    public TestRangeDouble(int nrServers) throws IOException {
        super(nrServers);
        factory = new ZKPHFactory(HOST, ZK_PORT);
    }

    @Before
    public void before() {
        pht = factory.createPHTreeRangeSet(2, 64);

        e1 = new PHREntry(new double[]{2,3}, new double[]{7,8});
        e2 = new PHREntry(new double[]{-1,-2}, new double[]{1,2});
        e3 = new PHREntry(new double[]{-7,-8}, new double[]{-2,-3});
    }

    @Test
    public void testInsertContains() {
        assertFalse(pht.contains(e1));
        assertFalse(pht.contains(e2));
        assertFalse(pht.contains(e3));

        pht.insert(e1);
        assertTrue(pht.contains(e1));
        assertFalse(pht.contains(e2));
        assertFalse(pht.contains(e3));

        pht.insert(e2);
        assertTrue(pht.contains(e1));
        assertTrue(pht.contains(e2));
        assertFalse(pht.contains(e3));

        pht.insert(e3);
        assertTrue(pht.contains(e1));
        assertTrue(pht.contains(e2));
        assertTrue(pht.contains(e3));
    }

    @Test
    public void testDeleteContains() {
        pht.insert(e1);
        pht.insert(e2);
        pht.insert(e3);

        pht.delete(e3);
        assertTrue(pht.contains(e1));
        assertTrue(pht.contains(e2));
        assertFalse(pht.contains(e3));

        pht.delete(e2);
        assertTrue(pht.contains(e1));
        assertFalse(pht.contains(e2));
        assertFalse(pht.contains(e3));

        pht.delete(e1);
        assertFalse(pht.contains(e1));
        assertFalse(pht.contains(e2));
        assertFalse(pht.contains(e3));
    }

    @Test
    public void testQueryInclude() {
        pht.insert(e1);
        pht.insert(e2);
        pht.insert(e3);

        Iterator<PHREntry> iter = pht.queryInclude(e1);
        assertEquals(e1, iter.next());
        assertFalse(iter.hasNext());

        iter = pht.queryInclude(e2);
        assertEquals(e2, iter.next());
        assertFalse(iter.hasNext());

        iter = pht.queryInclude(e3);
        assertEquals(e3, iter.next());
        assertFalse(iter.hasNext());


        iter = pht.queryInclude(new double[]{-1, -1}, new double[]{1, 1});
        assertFalse(iter.hasNext());

        iter = pht.queryInclude(new double[]{-5, -5}, new double[]{-4, -4});
        assertFalse(iter.hasNext());

        iter = pht.queryInclude(new double[]{4, 4}, new double[]{5, 5});
        assertFalse(iter.hasNext());

        iter = pht.queryInclude(new double[]{-5, -5}, new double[]{5, 5});
        assertEquals(e2, iter.next());
        assertFalse(iter.hasNext());

        iter = pht.queryInclude(new double[]{-15, -15}, new double[]{15, 15});
        //This order can change...
        assertEquals(e1, iter.next());
        assertEquals(e2, iter.next());
        assertEquals(e3, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testQueryIntersect() {
        pht.insert(e1);
        pht.insert(e2);
        pht.insert(e3);

        Iterator<PHREntry> iter = pht.queryIntersect(e1);
        assertEquals(e1, iter.next());
        assertFalse(iter.hasNext());

        iter = pht.queryIntersect(e2);
        assertEquals(e2, iter.next());
        assertFalse(iter.hasNext());

        iter = pht.queryIntersect(e3);
        assertEquals(e3, iter.next());
        assertFalse(iter.hasNext());


        iter = pht.queryIntersect(new double[]{-1, -1}, new double[]{1, 1});
        assertEquals(e2, iter.next());
        assertFalse(iter.hasNext());

        iter = pht.queryIntersect(new double[]{-5, -5}, new double[]{-4, -4});
        assertEquals(e3, iter.next());
        assertFalse(iter.hasNext());

        iter = pht.queryIntersect(new double[]{4, 4}, new double[]{5, 5});
        assertEquals(e1, iter.next());
        assertFalse(iter.hasNext());

        iter = pht.queryIntersect(new double[]{-5, -5}, new double[]{5, 5});
        //This order can change...
        assertEquals(e1, iter.next());
        assertEquals(e2, iter.next());
        assertEquals(e3, iter.next());
        assertFalse(iter.hasNext());

        iter = pht.queryIntersect(new double[]{-15, -15}, new double[]{15, 15});
        //This order can change...
        assertEquals(e1, iter.next());
        assertEquals(e2, iter.next());
        assertEquals(e3, iter.next());
        assertFalse(iter.hasNext());
    }

}
