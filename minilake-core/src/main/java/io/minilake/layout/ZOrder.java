package io.minilake.layout;

/**
 * Morton (Z-order) keys: interleave the bits of several small integer coordinates into one long,
 * so sorting by the key keeps rows that are close in every dimension close in the file layout.
 *
 * <p>Coordinates are expected to be rank buckets in {@code [0, 2^bitsPerDim)}, not raw values: raw
 * values are skewed and of different scales, and interleaving them would let one column dominate.
 */
public final class ZOrder {
  private ZOrder() {}

  /** Largest bits-per-dimension that fits {@code dims} coordinates in a non-negative long. */
  public static int maxBitsPerDim(int dims) {
    if (dims < 1) throw new IllegalArgumentException("need at least one dimension");
    return Math.min(31, 63 / dims);
  }

  /**
   * Bit {@code b} of coordinate {@code d} lands at position {@code b * dims + (dims - 1 - d)}: the
   * most significant bits of every coordinate come first, and the first column wins ties.
   */
  public static long interleave(int[] coords, int bitsPerDim) {
    int dims = coords.length;
    if (bitsPerDim < 1 || bitsPerDim > maxBitsPerDim(dims)) {
      throw new IllegalArgumentException(bitsPerDim + " bits x " + dims + " dims does not fit in 63 bits");
    }
    long key = 0;
    for (int d = 0; d < dims; d++) {
      int c = coords[d];
      if (c < 0 || (bitsPerDim < 31 && c >= (1 << bitsPerDim))) {
        throw new IllegalArgumentException("coordinate " + c + " out of range for " + bitsPerDim + " bits");
      }
      for (int b = 0; b < bitsPerDim; b++) {
        if ((c >>> b & 1) != 0) key |= 1L << (b * dims + (dims - 1 - d));
      }
    }
    return key;
  }
}
