package ch.ethz.globis.distindex.codec;

/**
 * Merge the field encoder and decoder interfaces into a more compact interface.
 * @param <V>
 */
public interface FieldEncoderDecoder<V> extends FieldEncoder<V>, FieldDecoder<V> {

}
