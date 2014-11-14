package ch.ethz.globis.distindex.mapping.bst;

public interface KeyConverter<K> {

    public String convert(K key);

    public String getBitPrefix(K start, K end);

    public boolean isBitSet(K key, int position);
}
