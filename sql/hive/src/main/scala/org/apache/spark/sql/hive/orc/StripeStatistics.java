package org.apache.spark.sql.hive.orc;

import org.apache.hadoop.hive.ql.io.orc.ColumnStatistics;

import java.util.List;

public class StripeStatistics {
  private final List<OrcProto.ColumnStatistics> cs;

  StripeStatistics(List<OrcProto.ColumnStatistics> list) {
    this.cs = list;
  }

  /**
   * Return list of column statistics
   *
   * @return column stats
   */
  public ColumnStatistics[] getColumnStatistics() {
    ColumnStatistics[] result = new ColumnStatistics[cs.size()];
    for (int i = 0; i < result.length; ++i) {
      result[i] = ColumnStatisticsImpl2.deserialize(cs.get(i));
    }
    return result;
  }
}