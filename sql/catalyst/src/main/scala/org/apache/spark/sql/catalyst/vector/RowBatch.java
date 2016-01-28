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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

public class RowBatch implements Serializable {
  public int numCols; // number of columns
  public final int capacity;
  public int size; // number of rows that qualify
  public int[] selected; // array of selected rows
  public boolean selectedInUse; // if selected is valid
  public ColumnVector[] columns;

  public DataType[] fieldTypes;
  public List<String> colNames;

  public UTF8String str = new UTF8String();

  public boolean endOfFile;

  public static final int DEFAULT_SIZE = 1024;

  public RowBatch(int numCols, int capacity) {
    this.numCols = numCols;
    this.capacity = capacity;
    size = capacity;
    selected = new int[capacity];
    selectedInUse = false;
    columns = new ColumnVector[numCols];
  }

  public RowBatch(int numCols) {
    this(numCols, DEFAULT_SIZE);
  }

  public void reset() {
    selectedInUse = false;
    size = capacity;
    endOfFile = false;
    for (ColumnVector col : columns) {
      col.reset();
    }
  }

  public int[] bm() {
    int tmp[] = new int[capacity];
    if (!selectedInUse) {
      Arrays.fill(tmp, 1);
    } else {
      Arrays.fill(tmp, 0);
      for (int i = 0; i < size; i ++) {
        tmp[selected[i]] = 1;
      }
    }
    return tmp;
  }

  public Iterator<Row> rowIterator() {
    final int capacity = RowBatch.this.capacity;
    final Row row = new Row();
    return new Iterator<Row>() {
      int rowId = 0;
      int[] bm = bm();

      @Override
      public boolean hasNext() {
        while (rowId < capacity && bm[rowId] == 0) {
          rowId ++;
        }
        return rowId < capacity;
      }

      @Override
      public Row next() {
        while (rowId < capacity && bm[rowId] == 0) {
          rowId ++;
        }
        row.rowId = rowId++;
        return row;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public static RowBatch create(DataType[] dts, int capacity) {
    RowBatch rb = new RowBatch(dts.length, capacity);
    rb.fieldTypes = dts;
    for (int i = 0; i < dts.length; i ++) {
      rb.columns[i] = new ColumnVector(capacity, dts[i]);
    }
    return rb;
  }

  public static RowBatch create(DataType[] dts) {
    return create(dts, DEFAULT_SIZE);
  }

  public static RowBatch create(DataType[] dts, List<String> colNames) {
    RowBatch rb = create(dts, DEFAULT_SIZE);
    rb.colNames = colNames;
    return rb;
  }

  public final class Row extends InternalRow {
    private int rowId;

    @Override
    public int numFields() {
      return numCols;
    }

    @Override
    public InternalRow copy() {
//      Object[] o = {getUTF8String(0).clone(),  getUTF8String(1).clone(), getDouble(2)};
//      InternalRow row = new GenericInternalRow(o);
//      return row;
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean anyNull() {
      for (int i = 0; i < numFields(); i ++) {
        if (isNullAt(i)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return columns[ordinal].isNull[rowId];
    }

    @Override
    public boolean getBoolean(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public byte getByte(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public short getShort(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public int getInt(int ordinal) {
      return columns[ordinal].intVector[rowId];
    }

    @Override
    public long getLong(int ordinal) {
      return columns[ordinal].longVector[rowId];
    }

    @Override
    public float getFloat(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public double getDouble(int ordinal) {
      return columns[ordinal].doubleVector[rowId];
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
      throw new NotImplementedException();
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
      ColumnVector cv = columns[ordinal];
      str.update(cv.bytesVector[rowId], cv.starts[rowId], cv.lengths[rowId]);
      return str;
    }

    @Override
    public byte[] getBinary(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      throw new NotImplementedException();
    }

    @Override
    public ArrayData getArray(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public MapData getMap(int ordinal) {
      throw new NotImplementedException();
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
      throw new NotImplementedException();
    }
  }
}
