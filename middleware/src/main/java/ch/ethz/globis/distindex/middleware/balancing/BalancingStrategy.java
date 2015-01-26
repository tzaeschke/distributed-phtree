package ch.ethz.globis.distindex.middleware.balancing;

public interface BalancingStrategy {

    public void balance();

    public void balanceAndRemove();
}
