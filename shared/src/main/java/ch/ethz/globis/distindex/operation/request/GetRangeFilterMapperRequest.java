package ch.ethz.globis.distindex.operation.request;

import ch.ethz.globis.pht.PhPredicate;
import ch.ethz.globis.pht.util.PhMapper;

public class GetRangeFilterMapperRequest<K> extends GetRangeRequest<K> {

    private PhPredicate filter;
    private PhMapper mapper;
    private int maxResults;

    public GetRangeFilterMapperRequest(int id, byte opCode, String indexId, int mappingVersion,
                                       K start, K end,
                                       int maxResults,
                                       PhPredicate filter, PhMapper mapper) {
        super(id, opCode, indexId, mappingVersion, start, end);
        this.filter = filter;
        this.mapper = mapper;
        this.maxResults = maxResults;
    }

    public PhPredicate getFilter() {
        return filter;
    }

    public PhMapper getMapper() {
        return mapper;
    }

    public int getMaxResults() {
        return maxResults;
    }
}