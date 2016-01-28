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
import java.util.Arrays;

import org.apache.spark.sql.types.*;
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

  public UTF8String str = new UTF8String();

  public Object[] objectVector;

  public DataType dataType;

  public static final int intNullValue = 1;
  public static final int intOneValue = 1;

  public static final long longNullValue = 1L;
  public static final long longOneValue = 1L;

  public static final double doubleNullValue = Double.NaN;
  public static final double doubleOneValue = 1.0;

  public static final UTF8String UTF8StringNullValue = UTF8String.EMPTY_UTF8;
  public static final UTF8String UTF8StringOneValue = UTF8String.EMPTY_UTF8;

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
    } else {
      throw new UnsupportedOperationException(dt + "is Not supported yet");
      // objectVector = new Object[capacity];
    }
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

  /**
   * Resets the column to default state
   *  - fills the isNull array with false
   *  - sets noNulls to true
   *  - sets isRepeating to false
   */
  public void reset() {
    if (false == noNulls) {
      Arrays.fill(isNull, false);
    }
    noNulls = true;
    isRepeating = false;
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
}
