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

import java.io.Serializable;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

public abstract class ColumnVector implements Serializable {

  public DataType dataType;
  protected int capacity;

  // True if same value repeats for whole column vector. If so, vector[0] holds the repeating value.
  public boolean isRepeating;

  // If the whole column vector has no nulls, this is true, otherwise false.
  public boolean noNulls;
  public abstract void putNull(int rowId);
  public abstract void putNotNull(int rowId);
  public abstract boolean isNullAt(int rowId);
  public abstract void setNull(int rowId, boolean value);

  // ColumnVector do not necessarily have the same capacity as RowBatch, e.g. a literal cv.
  public int getCapacity() {
    return capacity;
  }

  // accessors for underlying arrays in On-Heap ColumnVector
  public abstract int[] getIntVector();
  public abstract long[] getLongVector();
  public abstract double[] getDoubleVector();
  public abstract byte[][] getBytesVector();
  public abstract int[] getStartsVector();
  public abstract int[] getLengthsVector();
  public abstract UnsafeRow[] getRowVector();

  public abstract long getDataNativeAddress();

  public UTF8String str = new UTF8String();
  public UTF8String anotherStr = new UTF8String();

  public ColumnVector(DataType type, int capacity) {
    this.dataType = type;
    this.capacity = capacity;
  }

  public abstract void reset();

  public abstract void putInt(int rowId, int value);
  public abstract void putLong(int rowId, long value);
  public abstract void putDouble(int rowId, double value);
  public abstract void putString(int rowId, UTF8String value);
  public abstract void putString(int rowId, String value);

  public abstract void putNulls(int destPos, int length);

  public abstract void putIntsRepeat(ColumnVector src, int srcPos, int destPos, int repeat);
  public abstract void putLongsRepeat(ColumnVector src, int srcPos, int destPos, int repeat);
  public abstract void putDoublesRepeat(ColumnVector src, int srcPos, int destPos, int repeat);
  public abstract void putStringsRepeat(ColumnVector src, int srcPos, int destPos, int repeat);

  public abstract void putIntsRepeats(ColumnVector src, int srcPos, int destPos, int repeat, int length);
  public abstract void putLongsRepeats(ColumnVector src, int srcPos, int destPos, int repeat, int length);
  public abstract void putDoublesRepeats(ColumnVector src, int srcPos, int destPos, int repeat, int length);
  public abstract void putStringsRepeats(ColumnVector src, int srcPos, int destPos, int repeat, int length);

  public abstract void putIntsRun(ColumnVector src, int srcPos, int destPos, int length);
  public abstract void putLongsRun(ColumnVector src, int srcPos, int destPos, int length);
  public abstract void putDoublesRun(ColumnVector src, int srcPos, int destPos, int length);
  public abstract void putStringsRun(ColumnVector src, int srcPos, int destPos, int length);

  public abstract void putIntsRuns(ColumnVector src, int srcPos, int destPos, int repeat, int length);
  public abstract void putLongsRuns(ColumnVector src, int srcPos, int destPos, int repeat, int length);
  public abstract void putDoublesRuns(ColumnVector src, int srcPos, int destPos, int repeat, int length);
  public abstract void putStringsRuns(ColumnVector src, int srcPos, int destPos, int repeat, int length);

  // TODO: is this necessary?
  public abstract void putIntsRunsWithStep(ColumnVector src, int srcPos, int destPos, int repeat, int length, int step);
  public abstract void putLongsRunsWithStep(ColumnVector src, int srcPos, int destPos, int repeat, int length, int step);
  public abstract void putDoublesRunsWithStep(ColumnVector src, int srcPos, int destPos, int repeat, int length, int step);
  public abstract void putStringsRunsWithStep(ColumnVector src, int srcPos, int destPos, int repeat, int length, int step);

  public abstract int getInt(int rowId);
  public abstract long getLong(int rowId);
  public abstract double getDouble(int rowId);
  public abstract UTF8String getString(int rowId);
  public abstract UTF8String getAnotherString(int rowId);
  public abstract int getStart(int rowId);
  public abstract int getLength(int rowId);

  public abstract void free();

  public abstract long memoryFootprintInBytes();

  public static final int intNullValue = 1;
  public static final int intOneValue = 1;

  public static final long longNullValue = 1L;
  public static final long longOneValue = 1L;

  public static final double doubleNullValue = Double.NaN;
  public static final double doubleOneValue = 1.0;

  public static final UTF8String UTF8StringNullValue = UTF8String.EMPTY_UTF8;
  public static final UTF8String UTF8StringOneValue = UTF8String.EMPTY_UTF8;

  protected static int DEFAULT_STR_LEN = 32;

  public static long estimateMemoryFootprint(DataType dataType, long length, MemoryMode mode) {
    if (mode == MemoryMode.ON_HEAP) {
      long mem = 64 /* field size */+ length * 1 + 16;
      if (dataType instanceof IntegerType) {
        mem += 4 * length + 16;
      } else if (dataType instanceof LongType) {
        mem += 8 * length + 16;
      } else if (dataType instanceof DoubleType) {
        mem += 8 * length + 16;
      } else if (dataType instanceof StringType) {
        mem += 4 * length + 16;
        mem += 4 * length + 16;
        mem += 4 * length + 16 /*bytes pointer array*/+ (16 + DEFAULT_STR_LEN) * length;
      }
      return mem;
    } else {
      long mem = 80 + length * 1;
      if (dataType instanceof IntegerType) {
        mem += 4 * length;
      } else if (dataType instanceof LongType || dataType instanceof DoubleType) {
        mem += 8 * length;
      } else if (dataType instanceof StringType) {
        mem += 4 * length;
        mem += 4 * length;
        mem += (DEFAULT_STR_LEN) * length;
      }
      return mem;
    }
  }

  public static void copyNulls(ColumnVector from, int fromIdx, ColumnVector to, int toIdx, int count) {
    for (int i = 0; i < count; i ++) {
      to.setNull(toIdx + i, from.isNullAt(fromIdx + i));
    }
  }
}
