package ch.ethz.globis.distindex.cluster;


import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.pht.nv.PhTreeNV;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class ReadTask implements Runnable {

    private PhTreeNV tree;
    private int nrEntries;

    public ReadTask(PHFactory factory, int nrEntries, int dim, int depth) {
        this.tree = factory.createPHTreeSet(dim, depth);
        this.nrEntries = nrEntries;
    }

    @Override
    public void run() {
        read(tree, nrEntries);
    }

    private void read(PhTreeNV tree, int nrEntries) {
        List<long[]> entries = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < nrEntries; i++) {
            entries.add(new long[]{
                    gaussianRandomValue(random), gaussianRandomValue(random)
            });
        }

        doRead(tree, entries);
    }

    private long gaussianRandomValue(Random random) {
        double r = random.nextGaussian();
        return (long) ((Long.MAX_VALUE - 1) * r);
    }

    private void doRead(PhTreeNV tree, List<long[]> points) {
        DateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start, end;
        boolean res = true;
        for (long[] point : points) {
            tree.insert(point);
            start = System.nanoTime();
            res &= tree.contains(point);
            end = System.nanoTime();
            System.out.println(date.format(new Date()) + ",end,get,"+ ((end - start) / 1000000.0));
        }
    }
}