package org.apache.spark.sql.hive.orc;

import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.spark.sql.catalyst.vector.ColumnVector;

import java.io.IOException;

interface IntegerReader {
  /**
   * Seek to the position provided by index.
   * @param index
   * @throws IOException
   */
  void seek(PositionProvider index) throws IOException;

  /**
   * Skip number of specified rows.
   * @param numValues
   * @throws IOException
   */
  void skip(long numValues) throws IOException;

  /**
   * Check if there are any more values left.
   * @return
   * @throws IOException
   */
  boolean hasNext() throws IOException;

  /**
   * Return the next available value.
   * @return
   * @throws IOException
   */
  long next() throws IOException;

  /**
   * Return the next available vector for values.
   * @return
   * @throws IOException
   */
  void nextIntVector(ColumnVector previous, long previousLen)
      throws IOException;

  void nextLongVector(ColumnVector previous, long previousLen)
      throws IOException;
}
