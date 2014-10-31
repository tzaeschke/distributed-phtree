package ch.ethz.globis.distindex.api;

/**
 *  Represents an entry of an {@link Index}
 *
 * @param <K>                           The type of the index key.
 * @param <V>                           The type of the index value.
 */
public class IndexEntry<K, V> {

    private K key;
    private V value;

    public IndexEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexEntry)) return false;

        IndexEntry that = (IndexEntry) o;

        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "IndexEntry{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}