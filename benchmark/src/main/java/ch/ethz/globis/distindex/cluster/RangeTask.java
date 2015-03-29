package ch.ethz.globis.distindex.cluster;

import ch.ethz.globis.distindex.client.pht.PHFactory;
import ch.ethz.globis.pht.PhTree;
import org.lwjgl.Sys;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class RangeTask implements Runnable{

    private PhTree tree;
    private int nrEntries;

    public RangeTask(PHFactory factory, int nrEntries, int dim, int depth) {
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
            tree.insert(point);
        }

        long offsetA, offsetB;
        long[] keyStart;
        long[] keyEnd;
        Random r = new Random();

        for (int i = 0; i < 1000; i++) {
            offsetA = r.nextInt(Integer.MAX_VALUE - 100);
            offsetB = r.nextInt(Integer.MAX_VALUE - 100);
            keyStart = new long[] {-offsetA, -offsetA};
            keyEnd = new long[] {offsetB, offsetB};
            start = System.currentTimeMillis();
            List<Object> objects = tree.queryAll(keyStart, keyEnd);
            end = System.currentTimeMillis();
            System.out.println(date.format(new Date()) + ",end,range,"+ (end - start) + objects.size());
        }
    }
}