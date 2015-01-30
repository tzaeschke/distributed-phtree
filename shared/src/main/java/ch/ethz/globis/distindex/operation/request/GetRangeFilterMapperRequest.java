package ch.ethz.globis.distindex.operation.request;

import ch.ethz.globis.pht.PhMapper;
import ch.ethz.globis.pht.PhPredicate;

public class GetRangeFilterMapperRequest<K> extends GetRangeRequest<K> {

    private PhPredicate filter;
    private PhMapper mapper;

    public GetRangeFilterMapperRequest(int id, byte opCode, String indexId, int mappingVersion,
                                       K start, K end,
                                       PhPredicate filter, PhMapper mapper) {
        super(id, opCode, indexId, mappingVersion, start, end);
        this.filter = filter;
        this.mapper = mapper;
    }

    public PhPredicate getFilter() {
        return filter;
    }

    public PhMapper getMapper() {
        return mapper;
    }
}