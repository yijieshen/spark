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

package org.apache.spark.sql.hive.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.*;

import org.apache.spark.sql.catalyst.vector.RowBatch;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.List;


public class VectorizedOrcInputFormat extends FileInputFormat<Void, RowBatch> {

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    OrcInputFormat oif = new OrcInputFormat();
    InputSplit[] splits = oif.getSplits(job, numSplits);
    return splits;
  }

  static class VectorizedOrcRecordReader implements RecordReader<Void, RowBatch> {
    private final long offset;
    private final long length;

    private final OrcFileReader fileReader;
    private final OrcRowBatchReader batchReader;
    private final List<String> colsToInclude;

    private final int defaultBatchCapacity;

    private float progress = 0.0f;
    private boolean addPartitionCols = true;

    private RowBatch rowBatch = null;

    VectorizedOrcRecordReader(
        OrcFileReader fileReader, Configuration conf, FileSplit split) throws IOException {
      this.offset = split.getStart();
      this.length = split.getLength();
      this.fileReader = fileReader;
      this.batchReader = createReaderFromFile(fileReader, conf, offset, length);
      this.colsToInclude = ColumnProjectionUtils.getReadColumnNames(conf);
      this.defaultBatchCapacity =
        conf.getInt(RowBatch.SPARK_SQL_VECTORIZE_BATCH_CAPACITY, RowBatch.DEFAULT_CAPACITY);
    }

    @Override
    public boolean next(Void key, RowBatch value) throws IOException {
      if (!batchReader.hasNext()) {
        return false;
      }
      try {
        batchReader.next(value);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      progress = batchReader.getProgress();
      return true;
    }

    @Override
    public Void createKey() {
      return null;
    }

    @Override
    public RowBatch createValue() {
      StructType fileOrigin = (StructType) fileReader.getDataType();
      DataType[] dts = new DataType[colsToInclude.size()];
      for (int i = 0; i < colsToInclude.size(); i ++) {
        dts[i] = fileOrigin.apply(colsToInclude.get(i)).dataType();
      }
      rowBatch = RowBatch.create(dts, colsToInclude, defaultBatchCapacity);
      return rowBatch;
    }

    @Override
    public long getPos() throws IOException {
      return offset + (long) (progress * length);
    }

    @Override
    public void close() throws IOException {
      batchReader.close();
    }

    @Override
    public float getProgress() throws IOException {
      return progress;
    }
  }

  @Override
  public RecordReader<Void, RowBatch> getRecordReader(
      InputSplit split, JobConf conf, Reporter reporter) throws IOException {
    FileSplit fSplit = (FileSplit) split;
    reporter.setStatus(fSplit.toString());
    Path path = fSplit.getPath();
    OrcFileReaderImpl.ReaderOptions opts = new OrcFileReaderImpl.ReaderOptions(conf);
    OrcFileReader fileReader = new OrcFileReaderImpl(path, opts);
    return new VectorizedOrcRecordReader(fileReader, conf, fSplit);
  }

  public static OrcRowBatchReader createReaderFromFile(
      OrcFileReader reader,
      Configuration conf,
      long offset,
      long length) throws IOException {
    OrcFileReader.Options options = new OrcFileReader.Options().range(offset, length);
    List<OrcProto.Type> types = reader.getTypes();
    options.include(genIncludedColumns(types, conf));
    setSearchArgument(options, types, conf);
    return reader.getRowBatchReader(options);
  }


  public static boolean[] genIncludedColumns(
      List<OrcProto.Type> types, Configuration conf) {
    if (!ColumnProjectionUtils.isReadAllColumns(conf)) {
      List<Integer> included = ColumnProjectionUtils.getReadColumnIDs(conf);
      return genIncludedColumns(types, included);
    } else {
      return null;
    }
  }

  public static boolean[] genIncludedColumns(
      List<OrcProto.Type> types, List<Integer> included) {
    int rootColumn = getRootColumn();
    int numColumns = types.size() - rootColumn;
    boolean[] result = new boolean[numColumns];
    result[0] = true;
    OrcProto.Type root = types.get(rootColumn);
    for(int i=0; i < root.getSubtypesCount(); ++i) {
      if (included.contains(i)) {
        includeColumnRecursive(types, result, root.getSubtypes(i),
          rootColumn);
      }
    }
    return result;
  }

  private static int getRootColumn() {
    return 0;
  }

  private static void includeColumnRecursive(
      List<OrcProto.Type> types,
      boolean[] result,
      int typeId,
      int rootColumn) {
    result[typeId - rootColumn] = true;
    OrcProto.Type type = types.get(typeId);
    int children = type.getSubtypesCount();
    for(int i=0; i < children; ++i) {
      includeColumnRecursive(types, result, type.getSubtypes(i), rootColumn);
    }
  }

  static void setSearchArgument(
      OrcFileReader.Options options,
      List<OrcProto.Type> types,
      Configuration conf) {
    String columnNamesString = conf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR);
    if (columnNamesString == null) {
      LOG.debug("No ORC pushdown predicate - no column names");
      options.searchArgument(null, null);
      return;
    }
    SearchArgument sarg = SearchArgumentFactory.createFromConf(conf);
    if (sarg == null) {
      LOG.debug("No ORC pushdown predicate");
      options.searchArgument(null, null);
      return;
    }

    LOG.info("ORC pushdown predicate: " + sarg);
    options.searchArgument(sarg, getSargColumnNames(
        columnNamesString.split(","), types, options.getInclude()));
  }

  public static String[] getSargColumnNames(
      String[] originalColumnNames,
      List<OrcProto.Type> types,
      boolean[] includedColumns) {
    int rootColumn = getRootColumn();
    String[] columnNames = new String[types.size() - rootColumn];
    int i = 0;
    for(int columnId: types.get(rootColumn).getSubtypesList()) {
      if (includedColumns == null || includedColumns[columnId - rootColumn]) {
        // this is guaranteed to be positive because types only have children
        // ids greater than their own id.
        columnNames[columnId - rootColumn] = originalColumnNames[i++];
      }
    }
    return columnNames;
  }
}
