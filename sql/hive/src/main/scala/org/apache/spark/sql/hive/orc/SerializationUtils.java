package org.apache.spark.sql.hive.orc;

import org.apache.hadoop.hive.ql.io.orc.InStream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class SerializationUtils {

  private final static int BUFFER_SIZE = 64;
  private final byte[] readBuffer;
  private final byte[] writeBuffer;

  public SerializationUtils() {
    this.readBuffer = new byte[BUFFER_SIZE];
    this.writeBuffer = new byte[BUFFER_SIZE];
  }

  long readVulong(InputStream in) throws IOException {
    long result = 0;
    long b;
    int offset = 0;
    do {
      b = in.read();
      if (b == -1) {
        throw new EOFException("Reading Vulong past EOF");
      }
      result |= (0x7f & b) << offset;
      offset += 7;
    } while (b >= 0x80);
    return result;
  }

  long readVslong(InputStream in) throws IOException {
    long result = readVulong(in);
    return (result >>> 1) ^ -(result & 1);
  }


  enum FixedBitSizes {
    ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE,
    THIRTEEN, FOURTEEN, FIFTEEN, SIXTEEN, SEVENTEEN, EIGHTEEN, NINETEEN,
    TWENTY, TWENTYONE, TWENTYTWO, TWENTYTHREE, TWENTYFOUR, TWENTYSIX,
    TWENTYEIGHT, THIRTY, THIRTYTWO, FORTY, FORTYEIGHT, FIFTYSIX, SIXTYFOUR;
  }

  /**
   * Decodes the ordinal fixed bit value to actual fixed bit width value
   * @param n - encoded fixed bit width
   * @return decoded fixed bit width
   */
  int decodeBitWidth(int n) {
    if (n >= FixedBitSizes.ONE.ordinal()
        && n <= FixedBitSizes.TWENTYFOUR.ordinal()) {
      return n + 1;
    } else if (n == FixedBitSizes.TWENTYSIX.ordinal()) {
      return 26;
    } else if (n == FixedBitSizes.TWENTYEIGHT.ordinal()) {
      return 28;
    } else if (n == FixedBitSizes.THIRTY.ordinal()) {
      return 30;
    } else if (n == FixedBitSizes.THIRTYTWO.ordinal()) {
      return 32;
    } else if (n == FixedBitSizes.FORTY.ordinal()) {
      return 40;
    } else if (n == FixedBitSizes.FORTYEIGHT.ordinal()) {
      return 48;
    } else if (n == FixedBitSizes.FIFTYSIX.ordinal()) {
      return 56;
    } else {
      return 64;
    }
  }

  /**
   * Read bitpacked integers from input stream
   * @param buffer - input buffer
   * @param offset - offset
   * @param len - length
   * @param bitSize - bit width
   * @param input - input stream
   * @throws IOException
   */
  void readInts(long[] buffer, int offset, int len, int bitSize,
                InStream input) throws IOException {
    int bitsLeft = 0;
    int current = 0;

    switch (bitSize) {
      case 1:
        unrolledUnPack1(buffer, offset, len, input);
        return;
      case 2:
        unrolledUnPack2(buffer, offset, len, input);
        return;
      case 4:
        unrolledUnPack4(buffer, offset, len, input);
        return;
      case 8:
        unrolledUnPack8(buffer, offset, len, input);
        return;
      case 16:
        unrolledUnPack16(buffer, offset, len, input);
        return;
      case 24:
        unrolledUnPack24(buffer, offset, len, input);
        return;
      case 32:
        unrolledUnPack32(buffer, offset, len, input);
        return;
      case 40:
        unrolledUnPack40(buffer, offset, len, input);
        return;
      case 48:
        unrolledUnPack48(buffer, offset, len, input);
        return;
      case 56:
        unrolledUnPack56(buffer, offset, len, input);
        return;
      case 64:
        unrolledUnPack64(buffer, offset, len, input);
        return;
      default:
        break;
    }

    for(int i = offset; i < (offset + len); i++) {
      long result = 0;
      int bitsLeftToRead = bitSize;
      while (bitsLeftToRead > bitsLeft) {
        result <<= bitsLeft;
        result |= current & ((1 << bitsLeft) - 1);
        bitsLeftToRead -= bitsLeft;
        current = input.read();
        bitsLeft = 8;
      }

      // handle the left over bits
      if (bitsLeftToRead > 0) {
        result <<= bitsLeftToRead;
        bitsLeft -= bitsLeftToRead;
        result |= (current >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
      }
      buffer[i] = result;
    }
  }

  private void unrolledUnPack1(long[] buffer, int offset, int len,
                               InStream input) throws IOException {
    final int numHops = 8;
    final int remainder = len % numHops;
    final int endOffset = offset + len;
    final int endUnroll = endOffset - remainder;
    int val = 0;
    for (int i = offset; i < endUnroll; i = i + numHops) {
      val = input.read();
      buffer[i] = (val >>> 7) & 1;
      buffer[i + 1] = (val >>> 6) & 1;
      buffer[i + 2] = (val >>> 5) & 1;
      buffer[i + 3] = (val >>> 4) & 1;
      buffer[i + 4] = (val >>> 3) & 1;
      buffer[i + 5] = (val >>> 2) & 1;
      buffer[i + 6] = (val >>> 1) & 1;
      buffer[i + 7] = val & 1;
    }

    if (remainder > 0) {
      int startShift = 7;
      val = input.read();
      for (int i = endUnroll; i < endOffset; i++) {
        buffer[i] = (val >>> startShift) & 1;
        startShift -= 1;
      }
    }
  }

  private void unrolledUnPack2(long[] buffer, int offset, int len,
                               InStream input) throws IOException {
    final int numHops = 4;
    final int remainder = len % numHops;
    final int endOffset = offset + len;
    final int endUnroll = endOffset - remainder;
    int val = 0;
    for (int i = offset; i < endUnroll; i = i + numHops) {
      val = input.read();
      buffer[i] = (val >>> 6) & 3;
      buffer[i + 1] = (val >>> 4) & 3;
      buffer[i + 2] = (val >>> 2) & 3;
      buffer[i + 3] = val & 3;
    }

    if (remainder > 0) {
      int startShift = 6;
      val = input.read();
      for (int i = endUnroll; i < endOffset; i++) {
        buffer[i] = (val >>> startShift) & 3;
        startShift -= 2;
      }
    }
  }

  private void unrolledUnPack4(long[] buffer, int offset, int len,
                               InStream input) throws IOException {
    final int numHops = 2;
    final int remainder = len % numHops;
    final int endOffset = offset + len;
    final int endUnroll = endOffset - remainder;
    int val = 0;
    for (int i = offset; i < endUnroll; i = i + numHops) {
      val = input.read();
      buffer[i] = (val >>> 4) & 15;
      buffer[i + 1] = val & 15;
    }

    if (remainder > 0) {
      int startShift = 4;
      val = input.read();
      for (int i = endUnroll; i < endOffset; i++) {
        buffer[i] = (val >>> startShift) & 15;
        startShift -= 4;
      }
    }
  }

  private void unrolledUnPack8(long[] buffer, int offset, int len,
                               InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 1);
  }

  private void unrolledUnPack16(long[] buffer, int offset, int len,
                                InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 2);
  }

  private void unrolledUnPack24(long[] buffer, int offset, int len,
                                InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 3);
  }

  private void unrolledUnPack32(long[] buffer, int offset, int len,
                                InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 4);
  }

  private void unrolledUnPack40(long[] buffer, int offset, int len,
                                InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 5);
  }

  private void unrolledUnPack48(long[] buffer, int offset, int len,
                                InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 6);
  }

  private void unrolledUnPack56(long[] buffer, int offset, int len,
                                InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 7);
  }

  private void unrolledUnPack64(long[] buffer, int offset, int len,
                                InStream input) throws IOException {
    unrolledUnPackBytes(buffer, offset, len, input, 8);
  }

  private void unrolledUnPackBytes(long[] buffer, int offset, int len, InStream input, int numBytes)
      throws IOException {
    final int numHops = 8;
    final int remainder = len % numHops;
    final int endOffset = offset + len;
    final int endUnroll = endOffset - remainder;
    int i = offset;
    for (; i < endUnroll; i = i + numHops) {
      readLongBE(input, buffer, i, numHops, numBytes);
    }

    if (remainder > 0) {
      readRemainingLongs(buffer, i, input, remainder, numBytes);
    }
  }

  private void readRemainingLongs(long[] buffer, int offset, InStream input, int remainder,
                                  int numBytes) throws IOException {
    final int toRead = remainder * numBytes;
    // bulk read to buffer
    int bytesRead = input.read(readBuffer, 0, toRead);
    while (bytesRead != toRead) {
      bytesRead += input.read(readBuffer, bytesRead, toRead - bytesRead);
    }

    int idx = 0;
    switch (numBytes) {
      case 1:
        while (remainder > 0) {
          buffer[offset++] = readBuffer[idx] & 255;
          remainder--;
          idx++;
        }
        break;
      case 2:
        while (remainder > 0) {
          buffer[offset++] = readLongBE2(input, idx * 2);
          remainder--;
          idx++;
        }
        break;
      case 3:
        while (remainder > 0) {
          buffer[offset++] = readLongBE3(input, idx * 3);
          remainder--;
          idx++;
        }
        break;
      case 4:
        while (remainder > 0) {
          buffer[offset++] = readLongBE4(input, idx * 4);
          remainder--;
          idx++;
        }
        break;
      case 5:
        while (remainder > 0) {
          buffer[offset++] = readLongBE5(input, idx * 5);
          remainder--;
          idx++;
        }
        break;
      case 6:
        while (remainder > 0) {
          buffer[offset++] = readLongBE6(input, idx * 6);
          remainder--;
          idx++;
        }
        break;
      case 7:
        while (remainder > 0) {
          buffer[offset++] = readLongBE7(input, idx * 7);
          remainder--;
          idx++;
        }
        break;
      case 8:
        while (remainder > 0) {
          buffer[offset++] = readLongBE8(input, idx * 8);
          remainder--;
          idx++;
        }
        break;
      default:
        break;
    }
  }

  private void readLongBE(InStream in, long[] buffer, int start, int numHops, int numBytes)
      throws IOException {
    final int toRead = numHops * numBytes;
    // bulk read to buffer
    int bytesRead = in.read(readBuffer, 0, toRead);
    while (bytesRead != toRead) {
      bytesRead += in.read(readBuffer, bytesRead, toRead - bytesRead);
    }

    switch (numBytes) {
      case 1:
        buffer[start + 0] = readBuffer[0] & 255;
        buffer[start + 1] = readBuffer[1] & 255;
        buffer[start + 2] = readBuffer[2] & 255;
        buffer[start + 3] = readBuffer[3] & 255;
        buffer[start + 4] = readBuffer[4] & 255;
        buffer[start + 5] = readBuffer[5] & 255;
        buffer[start + 6] = readBuffer[6] & 255;
        buffer[start + 7] = readBuffer[7] & 255;
        break;
      case 2:
        buffer[start + 0] = readLongBE2(in, 0);
        buffer[start + 1] = readLongBE2(in, 2);
        buffer[start + 2] = readLongBE2(in, 4);
        buffer[start + 3] = readLongBE2(in, 6);
        buffer[start + 4] = readLongBE2(in, 8);
        buffer[start + 5] = readLongBE2(in, 10);
        buffer[start + 6] = readLongBE2(in, 12);
        buffer[start + 7] = readLongBE2(in, 14);
        break;
      case 3:
        buffer[start + 0] = readLongBE3(in, 0);
        buffer[start + 1] = readLongBE3(in, 3);
        buffer[start + 2] = readLongBE3(in, 6);
        buffer[start + 3] = readLongBE3(in, 9);
        buffer[start + 4] = readLongBE3(in, 12);
        buffer[start + 5] = readLongBE3(in, 15);
        buffer[start + 6] = readLongBE3(in, 18);
        buffer[start + 7] = readLongBE3(in, 21);
        break;
      case 4:
        buffer[start + 0] = readLongBE4(in, 0);
        buffer[start + 1] = readLongBE4(in, 4);
        buffer[start + 2] = readLongBE4(in, 8);
        buffer[start + 3] = readLongBE4(in, 12);
        buffer[start + 4] = readLongBE4(in, 16);
        buffer[start + 5] = readLongBE4(in, 20);
        buffer[start + 6] = readLongBE4(in, 24);
        buffer[start + 7] = readLongBE4(in, 28);
        break;
      case 5:
        buffer[start + 0] = readLongBE5(in, 0);
        buffer[start + 1] = readLongBE5(in, 5);
        buffer[start + 2] = readLongBE5(in, 10);
        buffer[start + 3] = readLongBE5(in, 15);
        buffer[start + 4] = readLongBE5(in, 20);
        buffer[start + 5] = readLongBE5(in, 25);
        buffer[start + 6] = readLongBE5(in, 30);
        buffer[start + 7] = readLongBE5(in, 35);
        break;
      case 6:
        buffer[start + 0] = readLongBE6(in, 0);
        buffer[start + 1] = readLongBE6(in, 6);
        buffer[start + 2] = readLongBE6(in, 12);
        buffer[start + 3] = readLongBE6(in, 18);
        buffer[start + 4] = readLongBE6(in, 24);
        buffer[start + 5] = readLongBE6(in, 30);
        buffer[start + 6] = readLongBE6(in, 36);
        buffer[start + 7] = readLongBE6(in, 42);
        break;
      case 7:
        buffer[start + 0] = readLongBE7(in, 0);
        buffer[start + 1] = readLongBE7(in, 7);
        buffer[start + 2] = readLongBE7(in, 14);
        buffer[start + 3] = readLongBE7(in, 21);
        buffer[start + 4] = readLongBE7(in, 28);
        buffer[start + 5] = readLongBE7(in, 35);
        buffer[start + 6] = readLongBE7(in, 42);
        buffer[start + 7] = readLongBE7(in, 49);
        break;
      case 8:
        buffer[start + 0] = readLongBE8(in, 0);
        buffer[start + 1] = readLongBE8(in, 8);
        buffer[start + 2] = readLongBE8(in, 16);
        buffer[start + 3] = readLongBE8(in, 24);
        buffer[start + 4] = readLongBE8(in, 32);
        buffer[start + 5] = readLongBE8(in, 40);
        buffer[start + 6] = readLongBE8(in, 48);
        buffer[start + 7] = readLongBE8(in, 56);
        break;
      default:
        break;
    }
  }

  private long readLongBE2(InStream in, int rbOffset) {
    return (((readBuffer[rbOffset] & 255) << 8)
        + ((readBuffer[rbOffset + 1] & 255) << 0));
  }

  private long readLongBE3(InStream in, int rbOffset) {
    return (((readBuffer[rbOffset] & 255) << 16)
        + ((readBuffer[rbOffset + 1] & 255) << 8)
        + ((readBuffer[rbOffset + 2] & 255) << 0));
  }

  private long readLongBE4(InStream in, int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 24)
        + ((readBuffer[rbOffset + 1] & 255) << 16)
        + ((readBuffer[rbOffset + 2] & 255) << 8)
        + ((readBuffer[rbOffset + 3] & 255) << 0));
  }

  private long readLongBE5(InStream in, int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 32)
        + ((long) (readBuffer[rbOffset + 1] & 255) << 24)
        + ((readBuffer[rbOffset + 2] & 255) << 16)
        + ((readBuffer[rbOffset + 3] & 255) << 8)
        + ((readBuffer[rbOffset + 4] & 255) << 0));
  }

  private long readLongBE6(InStream in, int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 40)
        + ((long) (readBuffer[rbOffset + 1] & 255) << 32)
        + ((long) (readBuffer[rbOffset + 2] & 255) << 24)
        + ((readBuffer[rbOffset + 3] & 255) << 16)
        + ((readBuffer[rbOffset + 4] & 255) << 8)
        + ((readBuffer[rbOffset + 5] & 255) << 0));
  }

  private long readLongBE7(InStream in, int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 48)
        + ((long) (readBuffer[rbOffset + 1] & 255) << 40)
        + ((long) (readBuffer[rbOffset + 2] & 255) << 32)
        + ((long) (readBuffer[rbOffset + 3] & 255) << 24)
        + ((readBuffer[rbOffset + 4] & 255) << 16)
        + ((readBuffer[rbOffset + 5] & 255) << 8)
        + ((readBuffer[rbOffset + 6] & 255) << 0));
  }

  private long readLongBE8(InStream in, int rbOffset) {
    return (((long) (readBuffer[rbOffset] & 255) << 56)
        + ((long) (readBuffer[rbOffset + 1] & 255) << 48)
        + ((long) (readBuffer[rbOffset + 2] & 255) << 40)
        + ((long) (readBuffer[rbOffset + 3] & 255) << 32)
        + ((long) (readBuffer[rbOffset + 4] & 255) << 24)
        + ((readBuffer[rbOffset + 5] & 255) << 16)
        + ((readBuffer[rbOffset + 6] & 255) << 8)
        + ((readBuffer[rbOffset + 7] & 255) << 0));
  }

  /**
   * Read n bytes in big endian order and convert to long
   * @return long value
   */
  long bytesToLongBE(InStream input, int n) throws IOException {
    long out = 0;
    long val = 0;
    while (n > 0) {
      n--;
      // store it in a long and then shift else integer overflow will occur
      val = input.read();
      out |= (val << (n * 8));
    }
    return out;
  }

  /**
   * zigzag decode the given value
   * @param val
   * @return zizag decoded value
   */
  long zigzagDecode(long val) {
    return (val >>> 1) ^ -(val & 1);
  }

  /**
   * For a given fixed bit this function will return the closest available fixed
   * bit
   * @param n
   * @return closest valid fixed bit
   */
  int getClosestFixedBits(int n) {
    if (n == 0) {
      return 1;
    }

    if (n >= 1 && n <= 24) {
      return n;
    } else if (n > 24 && n <= 26) {
      return 26;
    } else if (n > 26 && n <= 28) {
      return 28;
    } else if (n > 28 && n <= 30) {
      return 30;
    } else if (n > 30 && n <= 32) {
      return 32;
    } else if (n > 32 && n <= 40) {
      return 40;
    } else if (n > 40 && n <= 48) {
      return 48;
    } else if (n > 48 && n <= 56) {
      return 56;
    } else {
      return 64;
    }
  }

  double readDouble(InputStream in) throws IOException {
    return Double.longBitsToDouble(readLongLE(in));
  }

  long readLongLE(InputStream in) throws IOException {
    in.read(readBuffer, 0, 8);
    return (((readBuffer[0] & 0xff) << 0)
        + ((readBuffer[1] & 0xff) << 8)
        + ((readBuffer[2] & 0xff) << 16)
        + ((long) (readBuffer[3] & 0xff) << 24)
        + ((long) (readBuffer[4] & 0xff) << 32)
        + ((long) (readBuffer[5] & 0xff) << 40)
        + ((long) (readBuffer[6] & 0xff) << 48)
        + ((long) (readBuffer[7] & 0xff) << 56));
  }
}
