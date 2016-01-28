package org.apache.spark.sql.hive.orc;

import org.apache.spark.sql.catalyst.vector.RowBatch;

import java.io.IOException;

public interface OrcRowBatchReader {

  boolean hasNext() throws IOException;

  RowBatch next(RowBatch previous) throws IOException;

  float getProgress() throws IOException;

  void close() throws IOException;
}
