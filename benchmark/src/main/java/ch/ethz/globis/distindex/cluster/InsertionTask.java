package ch.ethz.globis.distindex.cluster;

import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.pht.PhTree;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;


public class InsertionTask implements Runnable {

    private PhTree tree;
    private int nrEntries;

    public InsertionTask(PHFactory factory, int nrEntries, int dim, int depth) {
        this.tree = factory.createPHTreeSet(dim, depth);
        this.nrEntries = nrEntries;
    }

    @Override
    public void run() {
        insert(tree, nrEntries);
    }

    private void insert(PhTree tree, int nrEntries) {
        List<long[]> entries = new ArrayList<long[]>();
        Random random = new Random();
        for (int i = 0; i < nrEntries; i++) {
            entries.add(new long[]{
                    gaussianRandomValue(random), gaussianRandomValue(random)
            });
        }

        doInsert(tree, entries);
    }

    private long gaussianRandomValue(Random random) {
        double r = random.nextGaussian();
        return (long) ((Long.MAX_VALUE - 1) * r);
    }

    private void doInsert(PhTree tree, List<long[]> points) {
        DateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start, end;
        for (long[] point : points) {
            start = System.nanoTime();
            tree.insert(point);
            end = System.nanoTime();
            System.out.println(date.format(new Date()) + ",end,insert,"+ ((end - start) / 1000000.0));
        }
    }
}