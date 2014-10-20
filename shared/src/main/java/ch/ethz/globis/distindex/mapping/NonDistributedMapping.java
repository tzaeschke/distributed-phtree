package ch.ethz.globis.distindex.mapping;

import java.util.ArrayList;
import java.util.List;

public class NonDistributedMapping implements KeyMapping<long[]> {

    private final String host;
    private final List<String> hostList;

    public NonDistributedMapping(String host) {
        this.host = host;
        this.hostList = new ArrayList<>();
        hostList.add(host);
    }

    @Override
    public String getHostId(long[] key) {
        return host;
    }

    @Override
    public List<String> getHostIds(long[] start, long[] end) {
        return hostList;
    }

    @Override
    public List<String> getHostIds() {
        return hostList;
    }

    @Override
    public void add(String host) {
        
    }
}
