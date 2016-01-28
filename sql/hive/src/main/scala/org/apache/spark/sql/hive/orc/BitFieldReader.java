package org.apache.spark.sql.hive.orc;

import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.spark.sql.catalyst.vector.ColumnVector;

import java.io.EOFException;
import java.io.IOException;

class BitFieldReader {
  private final RunLengthByteReader input;
  private final int bitSize;
  private int current;
  private int bitsLeft;
  private final int mask;

  BitFieldReader(
      InStream input,
      int bitSize) throws IOException {
    this.input = new RunLengthByteReader(input);
    this.bitSize = bitSize;
    mask = (1 << bitSize) - 1;
  }

  private void readByte() throws IOException {
    if (input.hasNext()) {
      current = 0xff & input.next();
      bitsLeft = 8;
    } else {
      throw new EOFException("Read past end of bit field from " + this);
    }
  }

  int next() throws IOException {
    int result = 0;
    int bitsLeftToRead = bitSize;
    while (bitsLeftToRead > bitsLeft) {
      result <<= bitsLeft;
      result |= current & ((1 << bitsLeft) - 1);
      bitsLeftToRead -= bitsLeft;
      readByte();
    }
    if (bitsLeftToRead > 0) {
      result <<= bitsLeftToRead;
      bitsLeft -= bitsLeftToRead;
      result |= (current >>> bitsLeft) & ((1 << bitsLeftToRead) - 1);
    }
    return result & mask;
  }

  void nextVector(ColumnVector previous, long previousLen)
      throws IOException {

    previous.isRepeating = true;
    for (int i = 0; i < previousLen; i++) {
      if (!previous.isNull[i]) {
        previous.longVector[i] = next();
      } else {
        // The default value of null for int types in vectorized
        // processing is 1, so set that if the value is null
        previous.longVector[i] = 1;
      }

      // The default value for nulls in Vectorization for int types is 1
      // and given that non null value can also be 1, we need to check for isNull also
      // when determining the isRepeating flag.
      if (previous.isRepeating
          && i > 0
          && ((previous.longVector[i - 1] != previous.longVector[i]) ||
              (previous.isNull[i - 1] != previous.isNull[i]))) {
        previous.isRepeating = false;
      }
    }
  }

  void seek(PositionProvider index) throws IOException {
    input.seek(index);
    int consumed = (int) index.getNext();
    if (consumed > 8) {
      throw new IllegalArgumentException("Seek past end of byte at " +
          consumed + " in " + input);
    } else if (consumed != 0) {
      readByte();
      bitsLeft = 8 - consumed;
    } else {
      bitsLeft = 0;
    }
  }

  void skip(long items) throws IOException {
    long totalBits = bitSize * items;
    if (bitsLeft >= totalBits) {
      bitsLeft -= totalBits;
    } else {
      totalBits -= bitsLeft;
      input.skip(totalBits / 8);
      current = input.next();
      bitsLeft = (int) (8 - (totalBits % 8));
    }
  }

  @Override
  public String toString() {
    return "bit reader current: " + current + " bits left: " + bitsLeft +
        " bit size: " + bitSize + " from " + input;
  }
}