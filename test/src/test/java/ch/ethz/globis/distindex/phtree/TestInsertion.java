package ch.ethz.globis.distindex.phtree;

import ch.ethz.globis.distindex.BaseParameterizedTest;
import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.pht.BitTools;
import ch.ethz.globis.pht.Bits;
import ch.ethz.globis.pht.PhTree;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestInsertion extends BaseParameterizedTest {

    private PHFactory factory;

    public TestInsertion(int nrServers) throws IOException {
        super(nrServers);
        factory = new PHFactory(HOST, ZK_PORT);
    }

    private void assertExists(PhTree i, long... l) {
        if (!i.contains(l)) {
            i.printTree();
            fail(Bits.toBinary(l, i.getDEPTH()));
        }
    }

    @Test
    public void test0() {
        //DIM=1, DEPTH=2
        PhTree i = factory.createPHTreeSet(1, 2);
        i.insert(0);
        assertTrue( i.contains(0) ) ;
    }


    @Test
    public void test1() {
        PhTree i = factory.createPHTreeSet(2, 8);
        i.insert(130, 226);
        assertTrue( i.contains(130, 226) ) ;
    }


    @Test
    public void test2() {
        PhTree i = factory.createPHTreeSet(1, 8);
        i.insert(165);
        i.insert(118);
        i.insert(110);
        assertExists(i, 110);
        i.insert(84);
        assertExists(i, 110);
        assertExists(i, 84);
    }

    @Test
    public void test3() {
        PhTree i = factory.createPHTreeSet(1, 8);
        i.insert(94);
        i.insert(165);
        assertExists(i, 165);
        i.insert(231);
        assertExists(i, 165);
        assertExists(i, 231);
        i.insert(84);
        assertExists(i, 84);
        i.insert(244);
        assertExists(i, 165);
        assertExists(i, 231);
        assertExists(i, 84);
        assertExists(i, 244);

        i.insert(198);
        i.insert(198);
    }


    @Test
    public void test4() {
        PhTree i = factory.createPHTreeSet(2, 8);
        i.insert(125, 237);
        assertExists(i, 125, 237);
        i.insert(97, 231);
        assertExists(i, 125, 237);
    }


    @Test
    public void test5() {
        PhTree i = factory.createPHTreeSet(2, 8);
        i.insert( 182, 169);
        assertExists(i, 182, 169);
        i.insert( 184, 160);
        assertExists(i, 182, 169);
    }

    @Test
    public void test6() {
        PhTree i = factory.createPHTreeSet(2, 16);
        i.insert(584, 64024);
        i.insert(11210, 64625);
        i.insert(18225, 34844);
        assertExists(i, 584, 64024);
        i.insert(13539, 56686);
        i.insert(8738, 51126);
        assertExists(i, 584, 64024);
    }

    @Test
    public void test7() {
        PhTree i = factory.createPHTreeSet(1, 8);
        i.insert(26);
        i.insert(80);
        //i.printTree();
        i.insert(90);
        assertExists(i, 26);
        assertExists(i, 80);
        assertExists(i, 90);
    }

    @Test
    public void test8() {
        PhTree i = factory.createPHTreeSet(2, 16);
        i.insert(20574, 9827);
        i.insert(54559, 55739);
        i.insert(52326, 13370);
        i.insert(6801, 2726);
        i.insert(11141, 9613);
        assertExists(i, 11141, 9613);
    }

    @Test
    public void test9() {
        PhTree i = factory.createPHTreeSet(2, 8);
        i.insert(134, 1);
        i.insert(191, 13);
        assertExists(i, 191, 13);
    }

    @Test
    public void test10() {
        PhTree i = factory.createPHTreeSet(2, 8);
        i.insert(79, 245);
        i.insert(7, 95);
        i.insert(89, 1);
        assertExists(i, 79, 245);
    }

    /**
     * This causes a linear-->HC switch, which resulted in corruption.
     */
    @Test
    public void test11() {
        PhTree i = factory.createPHTreeSet(2, 8);
        i.insert(47, 150);
        i.insert(126, 34);
        i.insert(187, 133);
        i.insert(218, 60);
        assertExists(i, 218, 60);
    }

    @Test
    public void test12() {
        PhTree i = factory.createPHTreeSet(2, 8);
        i.insert( 230, 74);
        i.insert( 30, 38);
        i.insert( 163, 54);
        i.insert( 204, 10);
        i.insert( 139, 119);
        i.insert( 176, 89);
        assertExists(i, 176, 89);
    }

    @Test
    public void test13() {
        PhTree i = factory.createPHTreeSet(3, 32);
        i.insert(2421495525L, 3660847787L, 3008816559L);
        assertExists(i, 2421495525L, 3660847787L, 3008816559L);
    }


    @Test
    public void test14() {
        PhTree i = factory.createPHTreeSet(2, 8);
        i.insert(100, 135);
        i.insert(33, 114);
        i.insert(238, 84);
        i.insert(80, 88);
        i.insert(215, 245);
        i.insert(247, 74);
        i.insert(16, 188);
        assertExists(i, 33, 114);

        assertExists(i, 100, 135);
        assertExists(i, 33, 114);
        assertExists(i, 238, 84);
        assertExists(i, 80, 88);
        assertExists(i, 215, 245);
        assertExists(i, 247, 74);
        assertExists(i, 16, 188);
    }

    @Test
    public void test64bit() {
        PhTree i = factory.createPHTreeSet(2, 64);
        i.insert(-100, 135);
        i.insert(33, -114);
        i.insert(238, 84);
        i.insert(-80, -88);
        assertExists(i, -100, 135);
        assertExists(i, 33, -114);
        assertExists(i, 238, 84);
        assertExists(i, -80, -88);
    }

    @Test
    public void test15() {
        PhTree ind = factory.createPHTreeSet(2, 16);
        Random R = new Random(1);
        int N = 20000;
        long[][] vals = new long[N][];
        for (int i = 0; i < N; i++) {
            long[] v = new long[]{R.nextInt(), R.nextInt()};
            vals[i] = v;
            if (ind.insert(v)) {
                //catch duplicates, maybe in future we should just skip them
                i--;
                continue;
            }
            assertTrue(ind.contains(v));
        }

        for (int i = 0; i < N; i++) {
            long[] v = vals[i];
            assertTrue(ind.delete(v));
            assertFalse(ind.contains(v));
            //try again.
            assertFalse(ind.delete(v));
            assertFalse(ind.contains(v));
        }
    }

    @Test
    public void test16BLHC() {
        PhTree ind = factory.createPHTreeSet(2, 16);
        ArrayList<long[]> vA = new ArrayList<long[]>();
        vA.add(new long[]{-1155869325, 431529176});
        vA.add(new long[]{1761283695, 1749940626});
        vA.add(new long[]{892128508, 155629808});
        vA.add(new long[]{-155886662, 685382526});
        for (long[] v: vA) {
            ind.insert(v);
        }
        for (long[] v: vA) {
            assertTrue(ind.contains(v));
        }
    }

    @Test
    public void test17() {
        //*******************************
        //*******************************
        //** Interesting, it works with 16 but fails with 17. Why, maybe because of hard coded
        //** 16 somewhere? There is one, for the splitting of blhc-pos into pageID and something
        //** else.
        //** See for example BLHC_POS_SPLIT_BITS used in blhcFromBIdPageOffs() which seems to
        //** assume that pageId uses at most 16 bits.
        //*******************************
        //*******************************
        //DEPTH=1 !!!
        PhTree ind = factory.createPHTreeSet(24, 1);
        ArrayList<long[]> vA = generateCluster17_18(34000, 24, 0.00001);
        for (long[] v: vA) {
            ind.insert(v);
        }
        for (long[] v: vA) {
            assertTrue(ind.contains(v));
        }
    }

    @Test
    public void test18() {
        PhTree ind = factory.createPHTreeSet(30, 64);
        ArrayList<long[]> vA = generateCluster17_18(40000, 30, 0.00001);
        for (long[] v: vA) {
            ind.insert(v);
        }
        for (long[] v: vA) {
            assertTrue(ind.contains(v));
        }
    }

    @Test
    public void testPostIncrementer() {
        System.err.println("Test currently disabled: TestIndexInsertion.testPostIncrementer()");
    }

    private ArrayList<long[]> generateCluster17_18(int N, int DIM, double CLUSTER_LEN) {
        double LEN = 1;
        Random R = new Random(0);
        int N_C = N/1000; //=points per cluster

        ArrayList<long[]> data = new ArrayList<long[]>(N);

        //loop over clusters
        for (int c = 0; c < 1000; c++) {
            double x0 = LEN * (c+0.5)/(double)1000; //=0.5/1000 ||  1.5/1000  ||  ...
            double yz0 = LEN * 0.5; //line is centered in all dimensions
            for (int p = 0; p < N_C; p++) {
                //int ii = (c*N_C+p) * DIM;
                long[] data2 = new long[DIM];
                data.add(data2);
                for (int d = 0; d < DIM; d++) {
                    double dd = LEN * (R.nextDouble()-0.5)*CLUSTER_LEN; //confine to small rectangle
                    if (d==0) {
                        dd += x0;
                    } else {
                        dd += yz0;
                    }
                    data2[d] = BitTools.toSortableLong(dd);
                }
            }
        }
        return data;
    }


    @Test
    public void test20_NewInsert() {
        PhTree i = factory.createPHTreeSet(3, 32);
        i.insert(3925440664L, 684358198, 1584853918);
        //i.printTree();
        i.insert(181670012, 3271367910L, 2679941640L);
        assertExists(i, 3925440664L, 684358198, 1584853918);
    }
}