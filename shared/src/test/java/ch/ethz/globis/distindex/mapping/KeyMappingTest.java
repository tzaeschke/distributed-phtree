package ch.ethz.globis.distindex.mapping;

import ch.ethz.globis.distindex.mapping.bst.MultidimMapping;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests KeyMapping operations.
 */
@RunWith(Parameterized.class)
public class KeyMappingTest {

    /** The mapping that will be used for this test */
    private KeyMapping<long[]> mapping;

    public KeyMappingTest(KeyMapping<long[]> mapping) {
        this.mapping = mapping;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { new MultidimMapping() }
        });
    }

    @Before
    public void prepareTest() {
        mapping.clear();
    }

    @Test
    public void testFindNext() {
        int size = 200;
        String[] hosts = new String[size];
        for (int i = 0; i < size; i++) {
            hosts[i] = String.valueOf(i);
            mapping.add(hosts[i]);
        }
        List<String> leaves = mapping.getHostIds();
        for (int i = 0; i < size - 1; i++) {
            String nextHostId = mapping.getNext(leaves.get(i));
            assertEquals(leaves.get(i + 1), nextHostId);
        }
        String hostId = leaves.get(size - 1);
        assertNull(null, mapping.getNext(hostId));
    }

    @Test
    public void testGetDepth() {
        int depth = 10;
        int size = (int) Math.pow(2, depth);
        String[] hosts = new String[size];
        for (int i = 0; i < size; i++) {
            hosts[i] = String.valueOf(i);
            mapping.add(hosts[i]);
        }
        for (String hostId : hosts) {
            assertEquals(depth + 1, mapping.getDepth(hostId));
        }
    }

    @Test
    public void testCreateRemove() {
        int size = 200;
        String[] hosts = new String[size];
        for (int i = 0; i < size; i++) {
            hosts[i] = String.valueOf(i);
            mapping.add(hosts[i]);
        }
        for (int i = 0; i < size; i++) {
            mapping.remove(hosts[i]);
            assertEquals(--size, mapping.size());
        }
        assertEquals(size, mapping.size());
    }

    @Test
    public void testSplittingCandidates() {
        int nrHosts = 200;
        String hostId;
        for (int i = 0; i < nrHosts; i++) {
            hostId = toHostId(i);
            mapping.add(hostId);
        }

        String candidate;
        for (int i = nrHosts - 1; i >= 0; i--) {
            hostId = toHostId(i);
            mapping.setSize(hostId, 2 * nrHosts - i);
            candidate = mapping.getHostForSplitting(toHostId(i + 1));
            assertEquals(hostId, candidate);
        }
    }

    @Test
    public void testSize() {
        int nrHosts = 200;
        String hostId;
        for (int i = 0; i < nrHosts; i++) {
            hostId = toHostId(i);
            mapping.add(hostId);
            assertEquals(i + 1, mapping.size());
        }
    }

    private String toHostId(int i) {
        return String.valueOf(i);
    }
}