package org.apache.spark.sql.execution.vector.aggregate;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.catalyst.vector.ColumnVector;
import org.apache.spark.sql.catalyst.vector.RowBatch;
import org.apache.spark.unsafe.types.UTF8String;

public class UnsafeRowVectorGen {

  /**
   * The default size for varlen columns. The row grows as necessary to accommodate the
   * largest column.
   */
  public static final int DEFAULT_VAR_LEN_SIZE = 16;

  private final int capacity;
  private final int numFields;
  private final int numVarFields;

  private BufferHolder[] holders;
  private UnsafeRow[] rows;
  private UnsafeRowWriter[] writers;
  private RowBatch rowBatch;

  public UnsafeRowVectorGen(int capacity, int numFields, int numVarFields) {
    this.capacity = capacity;
    this.numFields = numFields;
    this.numVarFields = numVarFields;
    holders = new BufferHolder[capacity];
    rows = new UnsafeRow[capacity];
    writers = new UnsafeRowWriter[capacity];
    initialize();
  }

  public void initialize() {
    int rowByteSize = UnsafeRow.calculateBitSetWidthInBytes(numFields)
      + 8 * numFields + numVarFields * DEFAULT_VAR_LEN_SIZE;
    for (int i = 0; i < capacity; i ++) {
      rows[i] = new UnsafeRow();
      writers[i] = new UnsafeRowWriter();
      holders[i] = new BufferHolder(rowByteSize);
    }
  }

  public void reset(RowBatch rowBatch) {
    this.rowBatch = rowBatch;
    for (int i = 0; i < capacity; i ++) {
      holders[i].reset();
      writers[i].initialize(holders[i], numFields);
    }
  }

  public void writeColumnInteger(int ordinal, ColumnVector intcv) {
    if (rowBatch.selectedInUse) {
      if (intcv.isRepeating && intcv.noNulls) {
        final int value = intcv.intVector[0];
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          writers[i].write(ordinal, value);
        }
      } else if (intcv.isRepeating) {
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          writers[i].setNullAt(ordinal);
        }
      } else if (intcv.noNulls) {
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          writers[i].write(ordinal, intcv.intVector[i]);
        }
      } else { // !isRepeating && hasNull
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          if (intcv.isNull[i]) {
            writers[i].setNullAt(ordinal);
          } else {
            writers[i].write(ordinal, intcv.intVector[i]);
          }
        }
      }
    } else {
      if (intcv.isRepeating && intcv.noNulls) {
        final int value = intcv.intVector[0];
        for (int i = 0; i < rowBatch.size; i ++) {
          writers[i].write(ordinal, value);
        }
      } else if (intcv.isRepeating) {
        for (int i = 0; i < rowBatch.size; i ++) {
          writers[i].setNullAt(ordinal);
        }
      } else if (intcv.noNulls) {
        for (int i = 0; i < rowBatch.size; i ++) {
          writers[i].write(ordinal, intcv.intVector[i]);
        }
      } else { // !isRepeating && hasNull
        for (int i = 0; i < rowBatch.size; i ++) {
          if (intcv.isNull[i]) {
            writers[i].setNullAt(ordinal);
          } else {
            writers[i].write(ordinal, intcv.intVector[i]);
          }
        }
      }
    }
  }

  public void writeColumnLong(int ordinal, ColumnVector longcv) {
    if (rowBatch.selectedInUse) {
      if (longcv.isRepeating && longcv.noNulls) {
        final long value = longcv.longVector[0];
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          writers[i].write(ordinal, value);
        }
      } else if (longcv.isRepeating) {
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          writers[i].setNullAt(ordinal);
        }
      } else if (longcv.noNulls) {
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          writers[i].write(ordinal, longcv.longVector[i]);
        }
      } else { // !isRepeating && hasNull
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          if (longcv.isNull[i]) {
            writers[i].setNullAt(ordinal);
          } else {
            writers[i].write(ordinal, longcv.longVector[i]);
          }
        }
      }
    } else {
      if (longcv.isRepeating && longcv.noNulls) {
        final long value = longcv.longVector[0];
        for (int i = 0; i < rowBatch.size; i ++) {
          writers[i].write(ordinal, value);
        }
      } else if (longcv.isRepeating) {
        for (int i = 0; i < rowBatch.size; i ++) {
          writers[i].setNullAt(ordinal);
        }
      } else if (longcv.noNulls) {
        for (int i = 0; i < rowBatch.size; i ++) {
          writers[i].write(ordinal, longcv.longVector[i]);
        }
      } else { // !isRepeating && hasNull
        for (int i = 0; i < rowBatch.size; i ++) {
          if (longcv.isNull[i]) {
            writers[i].setNullAt(ordinal);
          } else {
            writers[i].write(ordinal, longcv.longVector[i]);
          }
        }
      }
    }
  }

  public void writeColumnDouble(int ordinal, ColumnVector doublecv) {
    if (rowBatch.selectedInUse) {
      if (doublecv.isRepeating && doublecv.noNulls) {
        final double value = doublecv.doubleVector[0];
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          writers[i].write(ordinal, value);
        }
      } else if (doublecv.isRepeating) {
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          writers[i].setNullAt(ordinal);
        }
      } else if (doublecv.noNulls) {
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          writers[i].write(ordinal, doublecv.doubleVector[i]);
        }
      } else { // !isRepeating && hasNull
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          if (doublecv.isNull[i]) {
            writers[i].setNullAt(ordinal);
          } else {
            writers[i].write(ordinal, doublecv.doubleVector[i]);
          }
        }
      }
    } else {
      if (doublecv.isRepeating && doublecv.noNulls) {
        final double value = doublecv.doubleVector[0];
        for (int i = 0; i < rowBatch.size; i ++) {
          writers[i].write(ordinal, value);
        }
      } else if (doublecv.isRepeating) {
        for (int i = 0; i < rowBatch.size; i ++) {
          writers[i].setNullAt(ordinal);
        }
      } else if (doublecv.noNulls) {
        for (int i = 0; i < rowBatch.size; i ++) {
          writers[i].write(ordinal, doublecv.doubleVector[i]);
        }
      } else { // !isRepeating && hasNull
        for (int i = 0; i < rowBatch.size; i ++) {
          if (doublecv.isNull[i]) {
            writers[i].setNullAt(ordinal);
          } else {
            writers[i].write(ordinal, doublecv.doubleVector[i]);
          }
        }
      }
    }
  }

  public void writeColumnUTF8String(int ordinal, ColumnVector stringcv) {
    if (rowBatch.selectedInUse) {
      if (stringcv.isRepeating && stringcv.noNulls) {
        final UTF8String value = stringcv.stringVector[0];
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          writers[i].write(ordinal, value);
        }
      } else if (stringcv.isRepeating) {
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          writers[i].setNullAt(ordinal);
        }
      } else if (stringcv.noNulls) {
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          writers[i].write(ordinal, stringcv.stringVector[i]);
        }
      } else { // !isRepeating && hasNull
        for (int j = 0; j < rowBatch.size; j ++) {
          int i = rowBatch.selected[j];
          if (stringcv.isNull[i]) {
            writers[i].setNullAt(ordinal);
          } else {
            writers[i].write(ordinal, stringcv.stringVector[i]);
          }
        }
      }
    } else {
      if (stringcv.isRepeating && stringcv.noNulls) {
        final UTF8String value = stringcv.stringVector[0];
        for (int i = 0; i < rowBatch.size; i ++) {
          writers[i].write(ordinal, value);
        }
      } else if (stringcv.isRepeating) {
        for (int i = 0; i < rowBatch.size; i ++) {
          writers[i].setNullAt(ordinal);
        }
      } else if (stringcv.noNulls) {
        for (int i = 0; i < rowBatch.size; i ++) {
          writers[i].write(ordinal, stringcv.stringVector[i]);
        }
      } else { // !isRepeating && hasNull
        for (int i = 0; i < rowBatch.size; i ++) {
          if (stringcv.isNull[i]) {
            writers[i].setNullAt(ordinal);
          } else {
            writers[i].write(ordinal, stringcv.stringVector[i]);
          }
        }
      }
    }
  }

  public UnsafeRow[] evaluate() {
    for (int i = 0; i < capacity; i ++ ) {
      rows[i].pointTo(holders[i].buffer, numFields, holders[i].totalSize());
    }
    return rows;
  }
}
