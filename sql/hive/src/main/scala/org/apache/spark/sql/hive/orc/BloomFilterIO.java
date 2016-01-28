package org.apache.spark.sql.hive.orc;

import com.google.common.primitives.Longs;
import org.apache.hive.common.util.BloomFilter;

public class BloomFilterIO extends BloomFilter {

  public BloomFilterIO(long expectedEntries) {
    super(expectedEntries, DEFAULT_FPP);
  }

  public BloomFilterIO(long expectedEntries, double fpp) {
    super(expectedEntries, fpp);
  }

  /**
   * Initializes the BloomFilter from the given Orc BloomFilter
   */
  public BloomFilterIO(OrcProto.BloomFilter bloomFilter) {
    this.bitSet = new BitSet(Longs.toArray(bloomFilter.getBitsetList()));
    this.numHashFunctions = bloomFilter.getNumHashFunctions();
    this.numBits = (int) this.bitSet.bitSize();
  }
}
