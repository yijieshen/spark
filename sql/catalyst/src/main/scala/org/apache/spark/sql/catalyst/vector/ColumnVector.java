/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.vector;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

public class ColumnVector implements Serializable {

  /*
   * If hasNulls is true, then this array contains true if the value
   * is null, otherwise false. The array is always allocated, so a batch can be re-used
   * later and nulls added.
   */
  public boolean[] isNull;

  // If the whole column vector has no nulls, this is true, otherwise false.
  public boolean noNulls;

  /*
   * True if same value repeats for whole column vector.
   * If so, vector[0] holds the repeating value.
   */
  public boolean isRepeating;

  public int[] intVector;
  public long[] longVector;
  public double[] doubleVector;

  public byte[][] bytesVector;
  public int[] starts;
  public int[] lengths;

  public UnsafeRow[] rowVector;

  public UTF8String str = new UTF8String();

  public DataType dataType;

  public ByteBuffer values;
  public ByteBuffer nulls;
  public int nullCount;
  public int notNullCount;
  public int pos;

  public ColumnVector(int capacity, DataType dt, boolean ser) {
    prepareBuffers(capacity, dt);
    this.dataType = dt;
  }

  private void prepareBuffers(int capacity, DataType dt) {
    initialState();
    if (nulls == null) {
      nulls = ByteBuffer.allocate(1024);
      nulls.order(ByteOrder.BIG_ENDIAN);
    } else {
      nulls.clear();
    }
    if (values == null) {
      values = ByteBuffer.allocate(capacity * (dt instanceof IntegerType ? 4 : 8));
      values.order(ByteOrder.BIG_ENDIAN);
    } else {
      values.clear();
    }
  }

  private void initialState() {
    this.nullCount = 0;
    this.notNullCount = 0;
    this.pos = 0;
  }

  /**
   * Resets the column to default state
   *  - fills the isNull array with false
   *  - sets noNulls to true
   *  - sets isRepeating to false
   */
  public void reset() {
    if (false == noNulls && isNull != null) {
      Arrays.fill(isNull, false);
    }
    noNulls = true;
    isRepeating = false;
    initialState();
    if (nulls != null) {
      nulls.clear();
    }
    if (values != null) {
      values.clear();
    }
    if (bytesVector != null) {
      Arrays.fill(starts, 0);
      Arrays.fill(lengths, 0);
    }
  }

  public void writeToStream(DataOutputStream out) throws IOException {
    out.writeInt(nullCount);
    if (nullCount > 0) {
      nulls.flip();
      out.write(nulls.array(), 0, nulls.limit());
    }
    values.flip();
    out.writeInt(notNullCount);
    out.write(values.array(), 0, values.limit());
  }

  public int appendFromNullStream(DataInputStream in, int startIdx, int count) throws IOException {
    int nullCount = in.readInt();
    if (nullCount > 0) {
      this.noNulls = false;
      for (int i = 0; i < nullCount; i ++) {
        int nullPos = in.readInt();
        this.isNull[nullPos + startIdx] = true;
      }
    }
    return nullCount;
  }

  public int appendFromIntStream(DataInputStream in, int startIdx, int count) throws IOException {
    int notNullCount = in.readInt();
    if (noNulls) {
      for (int i = 0; i < notNullCount; i ++) {
        intVector[i + startIdx] = in.readInt();
      }
    } else {
      for (int i = 0; i < nullCount + notNullCount; i ++) {
        if (!isNull[i + startIdx]) {
          intVector[i + startIdx] = in.readInt();
        }
      }
    }
    return notNullCount;
  }

  public int appendFromLongStream(DataInputStream in, int startIdx, int count) throws IOException {
    int notNullCount = in.readInt();
    if (noNulls) {
      for (int i = 0; i < notNullCount; i ++) {
        longVector[i + startIdx] = in.readLong();
      }
    } else {
      for (int i = 0; i < nullCount + notNullCount; i ++) {
        if (!isNull[i + startIdx]) {
          longVector[i + startIdx] = in.readLong();
        }
      }
    }
    return notNullCount;
  }

  public int appendFromDoubleStream(DataInputStream in, int startIdx, int count) throws IOException {
    int notNullCount = in.readInt();
    if (noNulls) {
      for (int i = 0; i < notNullCount; i ++) {
        doubleVector[i + startIdx] = in.readDouble();
      }
    } else {
      for (int i = 0; i < nullCount + notNullCount; i ++) {
        if (!isNull[i + startIdx]) {
          doubleVector[i + startIdx] = in.readDouble();
        }
      }
    }
    return notNullCount;
  }

  public int appendFromStringStream(DataInputStream in, int startIdx, int count) throws IOException {
    int notNullCount = in.readInt();
    if (noNulls) {
      for (int i = 0; i < notNullCount; i ++) {
        int j = i + startIdx;
        lengths[j] = in.readInt();
        if (bytesVector[j] == null || bytesVector[j].length < lengths[j]) {
          bytesVector[j] = new byte[lengths[j]];
        }
        in.readFully(bytesVector[j], 0, lengths[j]);
      }
    } else {
      for (int i = 0; i < nullCount + notNullCount; i ++) {
        int j = i + startIdx;
        if (!isNull[j]) {
          lengths[j] = in.readInt();
          if (bytesVector[j] == null || bytesVector[j].length < lengths[j]) {
            bytesVector[j] = new byte[lengths[j]];
          }
          in.readFully(bytesVector[j], 0, lengths[j]);
        }
      }
    }
    return notNullCount;
  }

  public void readFromStream(DataInputStream in) throws IOException {
    reset();
    appendFromNullStream(in, 0, -1);
    if (dataType instanceof IntegerType) {
      appendFromIntStream(in, 0, -1);
    } else if (dataType instanceof LongType) {
      appendFromLongStream(in, 0, -1);
    } else if (dataType instanceof DoubleType) {
      appendFromDoubleStream(in, 0, -1);
    } else if (dataType instanceof StringType) {
      appendFromStringStream(in, 0, -1);
    } else {
      throw new UnsupportedOperationException(dataType + "is Not supported yet");
    }
  }

  public void putIntCV(ColumnVector src, Integer[] positions, int from, int length) {
    values = ensureFreeSpace(values, length * 4);
    if (src.noNulls) {
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        values.putInt(src.intVector[i]);
      }
      notNullCount += length;
      pos += length;
    } else {
      nulls = ensureFreeSpace(nulls, length * 4);
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        if (src.isNull[i]) {
          nullCount += 1;
          nulls.putInt(pos);
        } else {
          notNullCount += 1;
          values.putInt(src.intVector[i]);
        }
        pos += 1;
      }
    }
  }

  public void putLongCV(ColumnVector src, Integer[] positions, int from, int length) {
    values = ensureFreeSpace(values, length * 8);
    if (src.noNulls) {
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        values.putLong(src.longVector[i]);
      }
      notNullCount += length;
      pos += length;
    } else {
      nulls = ensureFreeSpace(nulls, length * 4);
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        if (src.isNull[i]) {
          nullCount += 1;
          nulls.putInt(pos);
        } else {
          notNullCount += 1;
          values.putLong(src.longVector[i]);
        }
        pos += 1;
      }
    }
  }

  public void putDoubleCV(ColumnVector src, Integer[] positions, int from, int length) {
    values = ensureFreeSpace(values, length * 8);
    if (src.noNulls) {
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        values.putDouble(src.doubleVector[i]);
      }
      notNullCount += length;
      pos += length;
    } else {
      nulls = ensureFreeSpace(nulls, length * 4);
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        if (src.isNull[i]) {
          nullCount += 1;
          nulls.putInt(pos);
        } else {
          notNullCount += 1;
          values.putDouble(src.doubleVector[i]);
        }
        pos += 1;
      }
    }
  }

  public void putStringCV(ColumnVector src, Integer[] positions, int from, int length) {
    values = ensureFreeSpace(values, length * 8);
    if (src.noNulls) {
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        values = ensureFreeSpace(values, 4 + src.lengths[i]);
        values.putInt(src.lengths[i]);
        writeTo(src, i, values);
      }
      notNullCount += length;
      pos += length;
    } else {
      nulls = ensureFreeSpace(nulls, length * 4);
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        if (src.isNull[i]) {
          nullCount += 1;
          nulls.putInt(pos);
        } else {
          notNullCount += 1;
          values = ensureFreeSpace(values, 4 + src.lengths[i]);
          values.putInt(src.lengths[i]);
          writeTo(src, i, values);
        }
        pos += 1;
      }
    }
  }

  private void writeToMemory(ColumnVector src, int i, Object target, long targetOffset) {
    Platform.copyMemory(src.bytesVector[i], src.starts[i] + Platform.BYTE_ARRAY_OFFSET,
      target, targetOffset, src.lengths[i]);
  }

  private void writeTo(ColumnVector src, int i, ByteBuffer buffer) {
    assert(buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(src, i, target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
    buffer.position(pos + src.lengths[i]);
  }

  private ByteBuffer ensureFreeSpace(ByteBuffer orig, int size) {
    if (orig.remaining() >= size) {
      return orig;
    } else {
      // grow in steps of initial size
      int capacity = orig.capacity();
      int newSize = capacity + Math.max(size, capacity);
      int pos = orig.position();

      return ByteBuffer
        .allocate(newSize)
        .order(ByteOrder.BIG_ENDIAN)
        .put(orig.array(), 0, pos);
    }
  }

  public void writeIntCVToStream(DataOutputStream out, Integer[] positions, int from, int length) throws IOException {
    if (noNulls) {
      out.writeInt(0);
      out.writeInt(length);
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        out.writeInt(intVector[i]);
      }
    } else {
      prepareBuffers(length, IntegerType$.MODULE$);
      putIntCV(this, positions, from, length);
      writeToStream(out);
    }
  }

  public void writeLongCVToStream(DataOutputStream out, Integer[] positions, int from, int length) throws IOException {
    if (noNulls) {
      out.writeInt(0);
      out.writeInt(length);
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        out.writeLong(longVector[i]);
      }
    } else {
      prepareBuffers(length, LongType$.MODULE$);
      putLongCV(this, positions, from, length);
      writeToStream(out);
    }
  }

  public void writeDoubleCVToStream(DataOutputStream out, Integer[] positions, int from, int length) throws IOException {
    if (noNulls) {
      out.writeInt(0);
      out.writeInt(length);
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        out.writeDouble(doubleVector[i]);
      }
    } else {
      prepareBuffers(length, DoubleType$.MODULE$);
      putDoubleCV(this, positions, from, length);
      writeToStream(out);
    }
  }

  public void writeStringCVToStream(DataOutputStream out, Integer[] positions, int from, int length) throws IOException {
    if (noNulls) {
      out.writeInt(0);
      out.writeInt(length);
      for (int j = from; j < from + length; j ++) {
        int i = positions[j];
        out.writeInt(lengths[i]);
        out.write(bytesVector[i], starts[i], lengths[i]);
      }
    } else {
      prepareBuffers(length, StringType$.MODULE$);
      putStringCV(this, positions, from, length);
      writeToStream(out);
    }
  }

  private ColumnVector(int capacity) {
    isNull = new boolean[capacity];
    noNulls = true;
    isRepeating = false;
  }

  /**
   * Constructor for super-class ColumnVector. This is not called directly,
   * but used to initialize inherited fields.
   *
   * @param capacity Vector length
   */
  public ColumnVector(int capacity, DataType dt) {
    this(capacity);
    dataType = dt;
    if (dt instanceof IntegerType) {
      intVector = new int[capacity];
    } else if (dt instanceof LongType) {
      longVector = new long[capacity];
    } else if (dt instanceof DoubleType) {
      doubleVector = new double[capacity];
    } else if (dt instanceof StringType) {
      bytesVector = new byte[capacity][];
      starts = new int[capacity];
      lengths = new int[capacity];
    } else if (dt instanceof StructType) {
      rowVector = new UnsafeRow[capacity];
    } else {
      throw new UnsupportedOperationException(dt + "is Not supported yet");
      // objectVector = new Object[capacity];
    }
  }

  public ColumnVector(ColumnVector cv) {
    dataType = cv.dataType;
    isNull = cv.isNull;
    noNulls = cv.noNulls;
    isRepeating = cv.isRepeating;
    intVector = cv.intVector;
    longVector = cv.longVector;
    doubleVector = cv.doubleVector;
    bytesVector = cv.bytesVector;
    rowVector = cv.rowVector;
    starts = new int[cv.starts.length];
    lengths = new int[cv.lengths.length];
    System.arraycopy(cv.starts, 0, starts, 0, cv.starts.length);
    System.arraycopy(cv.lengths, 0, lengths, 0, cv.lengths.length);
  }

  public static ColumnVector genIntegerColumnVector(int capacity) {
    ColumnVector cv = new ColumnVector(capacity);
    cv.dataType = IntegerType$.MODULE$;
    cv.intVector = new int[capacity];
    return cv;
  }

  public static ColumnVector genLongColumnVector(int capacity) {
    ColumnVector cv = new ColumnVector(capacity);
    cv.dataType = LongType$.MODULE$;
    cv.longVector = new long[capacity];
    return cv;
  }

  public static ColumnVector genDoubleColumnVector(int capacity) {
    ColumnVector cv = new ColumnVector(capacity);
    cv.dataType = DoubleType$.MODULE$;
    cv.doubleVector = new double[capacity];
    return cv;
  }

  public static ColumnVector genStringColumnVector(int capacity) {
    ColumnVector cv = new ColumnVector(capacity);
    cv.dataType = StringType$.MODULE$;
    cv.bytesVector = new byte[capacity][];
    cv.starts = new int[capacity];
    cv.lengths = new int[capacity];
    return cv;
  }

  public static ColumnVector genUnsafeRowColumnVector(int capacity, DataType dt) {
    ColumnVector cv = new ColumnVector(capacity);
    cv.dataType = dt; // dummy type here
    cv.rowVector = null;
    return cv;
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

  public void putNull(int rowId) {
    noNulls = false;
    isNull[rowId] = true;
  }

  public void putInt(int rowId, int value) {
    intVector[rowId] = value;
  }

  public void putLong(int rowId, long value) {
    longVector[rowId] = value;
  }

  public void putDouble(int rowId, double value) {
    doubleVector[rowId] = value;
  }

  public void putString(int rowId, UTF8String value) {
    bytesVector[rowId] = value.getBytes();
    starts[rowId] = 0;
    lengths[rowId] = value.numBytes();
  }

  public void putString(int rowId, String value) {
    byte[] bytes = value.getBytes();
    bytesVector[rowId] = bytes;
    starts[rowId] = 0;
    lengths[rowId] = bytes.length;
  }

  public UTF8String getString(int rowId) {
    return str.update(bytesVector[rowId], starts[rowId], lengths[rowId]);
  }

  public void put(int rowId, Object value) {
    if (dataType instanceof IntegerType) {
      putInt(rowId, (Integer) value);
    } else if (dataType instanceof LongType) {
      putLong(rowId, (Long) value);
    } else if (dataType instanceof DoubleType) {
      putDouble(rowId, (Double) value);
    } else if (dataType instanceof StringType) {
      putString(rowId, (UTF8String) value);
    } else {
      throw new UnsupportedOperationException(dataType + "is Not supported yet");
      // objectVector[rowId] = value;
    }
  }

  public static final int intNullValue = 1;
  public static final int intOneValue = 1;

  public static final long longNullValue = 1L;
  public static final long longOneValue = 1L;

  public static final double doubleNullValue = Double.NaN;
  public static final double doubleOneValue = 1.0;

  public static final UTF8String UTF8StringNullValue = UTF8String.EMPTY_UTF8;
  public static final UTF8String UTF8StringOneValue = UTF8String.EMPTY_UTF8;
}
