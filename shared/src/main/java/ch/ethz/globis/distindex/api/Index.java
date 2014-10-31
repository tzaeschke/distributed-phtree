package ch.ethz.globis.distindex.api;

/**
 * Representing an index that associates key objects to value objects.
 *
 * @param <K>                           The type of the index key.
 * @param <V>                           The type of the index value.
 */
public interface Index<K, V> {

    /**
     * Add an entry into the index, replacing the previous value with the same key.
     *
     * @param key                       The key of the new index entry.
     * @param value                     The value of the new index entry.
     * @return                          The previous value associated with the key or null of there was no
     *                                  previous value associated with this key.
     */
    public V put(K key, V value);

    /**
     * Check if the index contains a value associated with this key.
     * @param key                       The key to be searched.
     * @return                          True if there is a value in the index associated with this key, false otherwise.
     */
    public boolean contains(K key);

    /**
     * Retrieves a value from the index based on a key.
     * @param key                       The query key.
     * @return                          The value associated with the query key or null if there is no value associated
     *                                  with the query key.
     */
    public V get(K key);

    /**
     * Removes an entry from the index.
     *
     * @param key                       The query key.
     * @return                          The previous value associated with the key or null of there was no
     *                                  previous value associated with this key.
     */
    public V remove(K key);

    @Deprecated
    public IndexEntryList<K, V> getRange(K start, K end);

    /**
     * @return                          An iterator over all the entries in the index.
     */
    public IndexIterator<K, V> iterator();


    /**
     * Obtain an iterator over all the entries in the index that fall in the range [start, end] according to
     * the key ordering.
     * @param start                     The start of the key range.
     * @param end                       The end of the key range.
     * @return                          An iterator over the matched range.
     */
    public IndexIterator<K, V> query(K start, K end);
}
