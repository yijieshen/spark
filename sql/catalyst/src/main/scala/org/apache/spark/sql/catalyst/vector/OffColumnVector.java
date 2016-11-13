package org.apache.spark.sql.catalyst.vector;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;

public class OffColumnVector extends ColumnVector {

  private long isNull;
  private long data;

  private long starts;
  private long lengths;
  private int totalLen; // total length of all bytes appended
  private int allocated; // memory allocated to store string data

  private byte[] tmpByteArray;
  private byte[] anotherTmpByteArray;

  public OffColumnVector(DataType type, int capacity) {
    super(type, capacity);
    isNull = 0;
    data = 0;
    lengths = 0;
    starts = 0;
    totalLen = 0;

    doAllocate();
    noNulls = true;
    isRepeating = false;
  }

  private void doAllocate() {
    isNull = Platform.allocateMemory(capacity);
    Platform.setMemory(isNull, (byte) 0, capacity);
    if (dataType instanceof StringType) {
      this.lengths = Platform.allocateMemory(capacity * 4);
      this.starts = Platform.allocateMemory(capacity * 4);
      this.data = Platform.allocateMemory(capacity * DEFAULT_STR_LEN);
      allocated = capacity * DEFAULT_STR_LEN;
    } else if (dataType instanceof IntegerType) {
      this.data = Platform.allocateMemory(capacity * 4);
    } else if (dataType instanceof LongType || dataType instanceof DoubleType) {
      this.data = Platform.allocateMemory(capacity * 8);
    } else {
      throw new RuntimeException("Unhandled " + dataType);
    }
  }

  public OffColumnVector(ColumnVector other) {
    super(other.dataType, other.capacity);
    isNull = 0;
    data = 0;
    lengths = 0;
    starts = 0;
    totalLen = 0;

    doAllocate();
    noNulls = other.noNulls;
    isRepeating = other.isRepeating;

    if (dataType instanceof StringType) {
      for (int i = 0; i < capacity; i ++) {
        setNull(i, other.isNullAt(i));
        putString(i, other.getString(i));
      }
    } else if (dataType instanceof IntegerType) {
      for (int i = 0; i < capacity; i ++) {
        setNull(i, other.isNullAt(i));
        putInt(i, other.getInt(i));
      }
    } else if (dataType instanceof LongType){
      for (int i = 0; i < capacity; i ++) {
        setNull(i, other.isNullAt(i));
        putLong(i, other.getLong(i));
      }
    } else if (dataType instanceof DoubleType) {
      for (int i = 0; i < capacity; i ++) {
        setNull(i, other.isNullAt(i));
        putDouble(i, other.getDouble(i));
      }
    } else {
      throw new RuntimeException("Unhandled " + dataType);
    }
  }

  @Override
  public void putNull(int rowId) {
    Platform.putByte(null, isNull + rowId, (byte) 1);
    noNulls = false;
  }

  @Override
  public void putNotNull(int rowId) {
    Platform.putByte(null, isNull + rowId, (byte) 0);
  }

  @Override
  public void setNull(int rowId, boolean value) {
    Platform.putByte(null, isNull + rowId, (byte) (value? 1: 0));
  }

  @Override
  public boolean isNullAt(int rowId) {
    return Platform.getByte(null, isNull + rowId) == 1;
  }

  @Override
  public void reset() {
    isRepeating = false;
    if (false == noNulls) {
      Platform.setMemory(isNull, (byte) 0, capacity);
      noNulls = true;
    }
    totalLen = 0;
  }

  @Override
  public void putInt(int rowId, int value) {
    Platform.putInt(null, data + 4 * rowId, value);
  }

  @Override
  public void putLong(int rowId, long value) {
    Platform.putLong(null, data + 8 * rowId, value);
  }

  @Override
  public void putDouble(int rowId, double value) {
    Platform.putDouble(null, data + 8 * rowId, value);
  }

  public void copyFromByteBuffer(int rowId, ByteBuffer values, int length) {
    Platform.putInt(null, starts + 4 * rowId, totalLen);
    Platform.putInt(null, lengths + 4 * rowId, length);
    if (allocated - totalLen < length) {
      allocated *= 2;
      data = Platform.reallocateMemory(data, totalLen, allocated);
    }
    byte[] target = values.array();
    int offset = values.arrayOffset();
    int pos = values.position();
    Platform.copyMemory(
      target, Platform.BYTE_ARRAY_OFFSET + offset + pos, null, data + totalLen, length);
    values.position(pos + length);
    totalLen += length;
  }

  @Override
  public void putString(int rowId, UTF8String value) {
    int numBytes = value.numBytes();
    Platform.putInt(null, starts + 4 * rowId, totalLen);
    Platform.putInt(null, lengths + 4 * rowId, numBytes);
    if (allocated - totalLen < numBytes) {
      allocated *= 2;
      data = Platform.reallocateMemory(data, totalLen, allocated);
    }
    Platform.copyMemory(value.getBytes(), value.getBaseOffset(), null, data + totalLen, numBytes);
    totalLen += numBytes;
  }

  @Override
  public void putString(int rowId, String value) {
    byte[] bytes = value.getBytes();
    int numBytes = bytes.length;
    Platform.putInt(null, starts + 4 * rowId, totalLen);
    Platform.putInt(null, lengths + 4 * rowId, numBytes);
    if (allocated - totalLen < numBytes) {
      allocated *= 2;
      data = Platform.reallocateMemory(data, totalLen, allocated);
    }
    Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, null, data + totalLen, numBytes);
    totalLen += numBytes;
  }

  @Override
  public int getInt(int rowId) {
    return Platform.getInt(null, data + 4 * rowId);
  }

  @Override
  public long getLong(int rowId) {
    return Platform.getLong(null, data + 8 * rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return Platform.getDouble(null, data + 8 * rowId);
  }

  @Override
  public UTF8String getString(int rowId) {
    int length = Platform.getInt(null, lengths + 4 * rowId);
    int start = Platform.getInt(null, starts + 4 * rowId);
    if (tmpByteArray == null || tmpByteArray.length < length) {
      tmpByteArray = new byte[length];
    }
    Platform.copyMemory(null, data + start, tmpByteArray, Platform.BYTE_ARRAY_OFFSET, length);
    return str.update(tmpByteArray, 0, length);
  }

  @Override
  public UTF8String getAnotherString(int rowId) {
    int length = Platform.getInt(null, lengths + 4 * rowId);
    int start = Platform.getInt(null, starts + 4 * rowId);
    if (anotherTmpByteArray == null || anotherTmpByteArray.length < length) {
      anotherTmpByteArray = new byte[length];
    }
    Platform.copyMemory(null, data + start, anotherTmpByteArray, Platform.BYTE_ARRAY_OFFSET, length);
    return anotherStr.update(anotherTmpByteArray, 0, length);
  }

  @Override
  public int getStart(int rowId) {
    return Platform.getInt(null, starts + rowId * 4);
  }

  @Override
  public int getLength(int rowId) {
    return Platform.getInt(null, lengths + rowId * 4);
  }

  @Override
  public void free() {
    Platform.freeMemory(isNull);
    Platform.freeMemory(data);
    Platform.freeMemory(lengths);
    Platform.freeMemory(starts);
    isNull = 0;
    data = 0;
    lengths = 0;
    starts = 0;
    tmpByteArray = null;
  }

  @Override
  public long memoryFootprintInBytes() {
    long mem = 80 + capacity * 1;
    if (dataType instanceof IntegerType) {
      mem += 4 * capacity;
    } else if (dataType instanceof LongType || dataType instanceof DoubleType) {
      mem += 8 * capacity;
    } else if (dataType instanceof StringType) {
      mem += 4 * capacity;
      mem += 4 * capacity;
      mem += (DEFAULT_STR_LEN) * capacity;
    }
    return mem;
  }

  @Override
  public int[] getIntVector() {
    throw new RuntimeException("Cannot get int vector for off heap column");
  }

  @Override
  public long[] getLongVector() {
    throw new RuntimeException("Cannot get long vector for off heap column");
  }

  @Override
  public double[] getDoubleVector() {
    throw new RuntimeException("Cannot get double vector for off heap column");
  }

  @Override
  public byte[][] getBytesVector() {
    throw new RuntimeException("Cannot get bytes vector for off heap column");
  }

  @Override
  public int[] getStartsVector() {
    throw new RuntimeException("Cannot get starts vector for off heap column");
  }

  @Override
  public int[] getLengthsVector() {
    throw new RuntimeException("Cannot get lengths vector for off heap column");
  }

  @Override
  public UnsafeRow[] getRowVector() {
    throw new RuntimeException("Cannot get row vector for off heap column");
  }

  @Override
  public long getDataNativeAddress() {
    return data;
  }
}
