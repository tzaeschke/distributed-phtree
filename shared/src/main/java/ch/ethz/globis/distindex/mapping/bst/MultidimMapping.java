package ch.ethz.globis.distindex.mapping.bst;

import ch.ethz.globis.distindex.util.SerializerUtil;

public class MultidimMapping extends BSTMapping<long[]> {

    public MultidimMapping() {
        super(new LongArrayKeyConverter());
    }

    public byte[] serialize() {
        return SerializerUtil.getInstance().serialize(this);
    }

    public static MultidimMapping deserialize(byte[] data) {
        return SerializerUtil.getInstance().deserialize(data);
    }
}
