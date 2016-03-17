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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

public class RowBatch implements Serializable {
  public int numCols; // number of columns
  public final int capacity;
  public int size; // number of rows that qualify
  public int[] selected; // array of selected rows
  public boolean selectedInUse; // if selected is valid
  public ColumnVector[] columns;

  public Integer[] sorted; // array of sorted row indices
  public boolean sortedInUse; // if sorted is valid

  public DataType[] fieldTypes;
  public List<String> colNames;

  public UTF8String str = new UTF8String();

  public boolean endOfFile;

  public static final int DEFAULT_SIZE = 1024;

  /**
   * Constructor for serialization purpose only
   * @param dataTypes
   */
  public RowBatch(DataType[] dataTypes) {
    this.capacity = DEFAULT_SIZE;
    this.numCols = dataTypes.length;
    this.size = 0;
    this.selectedInUse = false;
    this.sortedInUse = false;
    this.fieldTypes = dataTypes;
    this.endOfFile = false;
    this.columns = new ColumnVector[numCols];
    for (int i = 0; i < numCols; i ++) {
      columns[i] = new ColumnVector(DEFAULT_SIZE, dataTypes[i], true);
    }
  }

  public void clear() {
    this.size = 0;
    this.endOfFile = false;
    for (ColumnVector col : columns) {
      col.clear();
    }
  }

  public void writeToStream(DataOutputStream out) throws IOException {
    for (ColumnVector col : columns) {
      col.writeToStream(out);
    }
  }

  public RowBatch(int numCols, int capacity) {
    this.numCols = numCols;
    this.capacity = capacity;
    size = capacity;
    selected = new int[capacity];
    selectedInUse = false;
    sorted = new Integer[capacity];
    sortedInUse = false;
    columns = new ColumnVector[numCols];
  }

  public RowBatch(int numCols) {
    this(numCols, DEFAULT_SIZE);
  }

  public void reset() {
    selectedInUse = false;
    sortedInUse = false;
    size = capacity;
    endOfFile = false;
    for (ColumnVector col : columns) {
      col.reset();
    }
  }

  public int[] getSelected() {
    if (selectedInUse) {
      return selected;
    } else {
      int[] newSelected = new int[size]; // TODO: should we update the rowbatch's one
      for (int i = 0; i < size; i ++) {
        newSelected[i] = i;
      }
      return newSelected;
    }
  }

  public Iterator<Row> rowIterator() {
    final Row row = new Row();
    return new Iterator<Row>() {
      int idxInSelectedArray = 0;
      int[] selected = getSelected();

      @Override
      public boolean hasNext() {
        return idxInSelectedArray < size;
      }

      @Override
      public Row next() {
        row.rowId = selected[idxInSelectedArray];
        idxInSelectedArray += 1;
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

  public void sort(Comparator<Integer> comparator) {
    sortedInUse = true;
    if (selectedInUse) {
      for (int i = 0; i < size; i ++) {
        sorted[i] = selected[i];
      }
    } else {
      for (int i = 0; i < size; i ++) {
        sorted[i] = i;
      }
    }
    Arrays.sort(sorted, 0, size, comparator);
  }

  public void sort(final int[] sortedBy) {
    Comparator<Integer> comparator =
        new Comparator<Integer>() {
          @Override
          public int compare(Integer i1, Integer i2) {
            return Integer.compare(sortedBy[i1], sortedBy[i2]);
          }
        };
    sort(comparator);
  }

  public final class Row extends InternalRow {
    private int rowId;

    @Override
    public int numFields() {
      return numCols;
    }

    @Override
    public InternalRow copy() {
      Object[] arr = new Object[numCols];
      for (int i = 0; i < numCols; i ++) {
        if (columns[i].dataType instanceof IntegerType) {
          arr[i] = getInt(i);
        } else if (columns[i].dataType instanceof LongType) {
          arr[i] = getLong(i);
        } else if (columns[i].dataType instanceof DoubleType) {
          arr[i] = getDouble(i);
        } else if (columns[i].dataType instanceof StringType) {
          arr[i] = getUTF8String(i).clone();
        }
      }
      return new GenericInternalRow(arr);
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
