package ch.ethz.globis.distindex.api;

import java.io.Closeable;
import java.util.Iterator;

/**
 *  An iterator over {@link} index-entry values.
 *
 * @param <K>                           The type of the index key.
 * @param <V>                           The type of the index value.
 */
public interface IndexIterator<K, V> extends Iterator<IndexEntry<K, V>>, Closeable, AutoCloseable{
}
