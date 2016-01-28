package org.apache.spark.sql.hive.orc;

import java.util.ArrayList;
import java.util.List;

public class Metadata {

  private final OrcProto.Metadata metadata;

  Metadata(OrcProto.Metadata m) {
    this.metadata = m;
  }

  /**
   * Return list of stripe level column statistics
   *
   * @return list of stripe statistics
   */
  public List<StripeStatistics> getStripeStatistics() {
    List<StripeStatistics> result = new ArrayList<StripeStatistics>();
    for (OrcProto.StripeStatistics ss : metadata.getStripeStatsList()) {
      result.add(new StripeStatistics(ss.getColStatsList()));
    }
    return result;
  }
}
