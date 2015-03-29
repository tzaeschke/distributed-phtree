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