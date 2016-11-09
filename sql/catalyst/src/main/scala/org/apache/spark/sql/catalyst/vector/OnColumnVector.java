package org.apache.spark.sql.catalyst.vector;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Arrays;

public class OnColumnVector extends ColumnVector {

  public boolean[] isNull;

  public int[] intVector;
  public long[] longVector;
  public double[] doubleVector;

  public byte[][] bytesVector;
  public int[] starts;
  public int[] lengths;

  public UnsafeRow[] rowVector;

  public OnColumnVector(DataType type, int capacity) {
    super(type, capacity);
    isNull = new boolean[capacity];
    if (type instanceof IntegerType) {
      intVector = new int[capacity];
    } else if (type instanceof LongType) {
      longVector = new long[capacity];
    } else if (type instanceof DoubleType) {
      doubleVector = new double[capacity];
    } else if (type instanceof StringType) {
      bytesVector = new byte[capacity][];
      starts = new int[capacity];
      lengths = new int[capacity];
    } else if (type instanceof StructType) {
      rowVector = new UnsafeRow[capacity];
    } else {
      throw new UnsupportedOperationException(type + "is Not supported yet");
    }
  }

  public OnColumnVector(int capacity, DataType type) {
    super(type, capacity);
    isNull = new boolean[capacity];
  }

  // Ctor only used by substring expression
  public OnColumnVector(ColumnVector otherCV) {
    super(otherCV.dataType, otherCV.capacity);
    OnColumnVector other = (OnColumnVector) otherCV;
    isNull = other.isNull;
    noNulls = other.noNulls;
    isRepeating = other.isRepeating;
    bytesVector = other.bytesVector;
    starts = new int[other.starts.length];
    lengths = new int[other.lengths.length];
    System.arraycopy(other.starts, 0, starts, 0, other.starts.length);
    System.arraycopy(other.lengths, 0, lengths, 0, other.lengths.length);
  }

  public static OnColumnVector genIntCV(int capacity) {
    OnColumnVector cv = new OnColumnVector(capacity, IntegerType$.MODULE$);
    cv.intVector = new int[capacity];
    return cv;
  }

  public static OnColumnVector genLongCV(int capacity) {
    OnColumnVector cv = new OnColumnVector(capacity, LongType$.MODULE$);
    cv.longVector = new long[capacity];
    return cv;
  }

  public static OnColumnVector genDoubleCV(int capacity) {
    OnColumnVector cv = new OnColumnVector(capacity, DoubleType$.MODULE$);
    cv.doubleVector = new double[capacity];
    return cv;
  }

  public static OnColumnVector genStringCV(int capacity) {
    OnColumnVector cv = new OnColumnVector(capacity, StringType$.MODULE$);
    cv.bytesVector = new byte[capacity][];
    cv.starts = new int[capacity];
    cv.lengths = new int[capacity];
    return cv;
  }

  public static OnColumnVector genUnsafeRowColumnVector(int capacity, DataType dt) {
    OnColumnVector cv = new OnColumnVector(capacity, dt);
    cv.rowVector = null;
    return cv;
  }

  @Override
  public void putNull(int rowId) {
    isNull[rowId] = true;
  }

  @Override
  public void putNotNull(int rowId) {
    isNull[rowId] = false;
  }

  @Override
  public void setNull(int rowId, boolean value) {
    isNull[rowId] = value;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return isNull[rowId];
  }

  @Override
  public int[] getIntVector() {
    return intVector;
  }

  @Override
  public long[] getLongVector() {
    return longVector;
  }

  @Override
  public double[] getDoubleVector() {
    return doubleVector;
  }

  @Override
  public byte[][] getBytesVector() {
    return bytesVector;
  }

  @Override
  public int[] getStartsVector() {
    return starts;
  }

  @Override
  public int[] getLengthsVector() {
    return lengths;
  }

  @Override
  public UnsafeRow[] getRowVector() {
    return rowVector;
  }

  @Override
  public long getDataNativeAddress() {
    throw new RuntimeException("Cannot get native address for on heap column");
  }

  @Override
  public void putInt(int rowId, int value) {
    intVector[rowId] = value;
  }

  @Override
  public void putLong(int rowId, long value) {
    longVector[rowId] = value;
  }

  @Override
  public void putDouble(int rowId, double value) {
    doubleVector[rowId] = value;
  }

  @Override
  public void putString(int rowId, UTF8String value) {
    bytesVector[rowId] = value.getBytes().clone();
    starts[rowId] = 0;
    lengths[rowId] = value.numBytes();
  }

  @Override
  public void putString(int rowId, String value) {
    byte[] bytes = value.getBytes();
    bytesVector[rowId] = bytes;
    starts[rowId] = 0;
    lengths[rowId] = bytes.length;
  }

  /** Set a field by reference.
   *
   * @param elementNum index within column vector to set
   * @param sourceBuf container of source data
   * @param start start byte position within source
   * @param length  length of source byte sequence
   */
  public void setRef(int elementNum, byte[] sourceBuf, int start, int length) {
    this.bytesVector[elementNum] = sourceBuf;
    this.starts[elementNum] = start;
    this.lengths[elementNum] = length;
  }

  @Override
  public int getInt(int rowId) {
    return intVector[rowId];
  }

  @Override
  public long getLong(int rowId) {
    return longVector[rowId];
  }

  @Override
  public double getDouble(int rowId) {
    return doubleVector[rowId];
  }

  @Override
  public UTF8String getString(int rowId) {
    return str.update(bytesVector[rowId], starts[rowId], lengths[rowId]);
  }

  @Override
  public UTF8String getAnotherString(int rowId) {
    return anotherStr.update(bytesVector[rowId], starts[rowId], lengths[rowId]);
  }

  @Override
  public int getStart(int rowId) {
    return starts[rowId];
  }

  @Override
  public int getLength(int rowId) {
    return lengths[rowId];
  }

  @Override
  public void reset() {
    isRepeating = false;
    if (false == noNulls && isNull != null) {
      Arrays.fill(isNull, false);
      noNulls = true;
    }
    if (bytesVector != null) {
      Arrays.fill(starts, 0);
      Arrays.fill(lengths, 0);
    }
  }

  @Override
  public void free() {
    isNull = null;
    intVector = null;
    doubleVector = null;
    longVector = null;
    if (bytesVector != null) {
      for (int i = 0; i < bytesVector.length; i ++) {
        bytesVector[i] = null;
      }
      bytesVector = null;
      starts = null;
      lengths = null;
    }
  }

  @Override
  public long memoryFootprintInBytes() {
    long mem = 64 /* field size */+ capacity * 1 + 16 /* isNull array*/;
    if (dataType instanceof IntegerType) {
      mem += 4 * capacity + 16;
    } else if (dataType instanceof LongType) {
      mem += 8 * capacity + 16;
    } else if (dataType instanceof DoubleType) {
      mem += 8 * capacity + 16;
    } else if (dataType instanceof StringType) {
      mem += 4 * capacity + 16;
      mem += 4 * capacity + 16;
      mem += 4 * capacity + 16 + (16 + DEFAULT_STR_LEN) * capacity;
    }
    return mem;
  }
}
