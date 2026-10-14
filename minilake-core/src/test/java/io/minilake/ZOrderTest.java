package io.minilake;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minilake.layout.ZOrder;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ZOrderTest {
  @Test
  void interleavesMostSignificantBitsFirst() {
    // x = 0b10, y = 0b01 -> bits (x1 y1 x0 y0) = 1 0 0 1
    assertEquals(0b1001, ZOrder.interleave(new int[] {0b10, 0b01}, 2));
    assertEquals(0b0110, ZOrder.interleave(new int[] {0b01, 0b10}, 2));
    assertEquals(5, ZOrder.interleave(new int[] {5}, 3), "one dimension is the identity");
  }

  @Test
  void isABijectionOnTheGrid() {
    Set<Long> keys = new HashSet<>();
    for (int x = 0; x < 16; x++) {
      for (int y = 0; y < 16; y++) keys.add(ZOrder.interleave(new int[] {x, y}, 4));
    }
    assertEquals(256, keys.size());
    assertTrue(keys.stream().allMatch(k -> k >= 0 && k < 256));
  }

  @Test
  void quadrantsAreContiguousRanges() {
    // Every key in the top-left 8x8 quadrant sorts before every key in any other quadrant.
    long maxTopLeft = 0, minOther = Long.MAX_VALUE;
    for (int x = 0; x < 16; x++) {
      for (int y = 0; y < 16; y++) {
        long k = ZOrder.interleave(new int[] {x, y}, 4);
        if (x < 8 && y < 8) maxTopLeft = Math.max(maxTopLeft, k);
        else minOther = Math.min(minOther, k);
      }
    }
    assertTrue(maxTopLeft < minOther);
  }

  @Test
  void rejectsWhatDoesNotFit() {
    assertEquals(21, ZOrder.maxBitsPerDim(3));
    assertThrows(IllegalArgumentException.class, () -> ZOrder.interleave(new int[] {0, 0, 0}, 22));
    assertThrows(IllegalArgumentException.class, () -> ZOrder.interleave(new int[] {4}, 2));
    assertThrows(IllegalArgumentException.class, () -> ZOrder.interleave(new int[] {-1}, 2));
  }
}
