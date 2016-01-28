package org.apache.spark.sql.hive.orc;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.Text;

import java.sql.Timestamp;
import java.util.Date;

public class ColumnStatisticsImpl2 implements ColumnStatistics {

  private static final class BooleanStatisticsImpl extends ColumnStatisticsImpl2
      implements BooleanColumnStatistics {
    private long trueCount = 0;

    BooleanStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.BucketStatistics bkt = stats.getBucketStatistics();
      trueCount = bkt.getCount(0);
    }

    BooleanStatisticsImpl() {
    }

    @Override
    public long getFalseCount() {
      return getNumberOfValues() - trueCount;
    }

    @Override
    public long getTrueCount() {
      return trueCount;
    }

    @Override
    public String toString() {
      return super.toString() + " true: " + trueCount;
    }
  }

  private static final class IntegerStatisticsImpl extends ColumnStatisticsImpl2
      implements IntegerColumnStatistics {

    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;
    private long sum = 0;
    private boolean hasMinimum = false;
    private boolean overflow = false;

    IntegerStatisticsImpl() {
    }

    IntegerStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.IntegerStatistics intStat = stats.getIntStatistics();
      if (intStat.hasMinimum()) {
        hasMinimum = true;
        minimum = intStat.getMinimum();
      }
      if (intStat.hasMaximum()) {
        maximum = intStat.getMaximum();
      }
      if (intStat.hasSum()) {
        sum = intStat.getSum();
      } else {
        overflow = true;
      }
    }

    @Override
    public long getMinimum() {
      return minimum;
    }

    @Override
    public long getMaximum() {
      return maximum;
    }

    @Override
    public boolean isSumDefined() {
      return !overflow;
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (hasMinimum) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
      }
      if (!overflow) {
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }
  }

  private static final class DoubleStatisticsImpl extends ColumnStatisticsImpl2
      implements DoubleColumnStatistics {
    private boolean hasMinimum = false;
    private double minimum = Double.MAX_VALUE;
    private double maximum = Double.MIN_VALUE;
    private double sum = 0;

    DoubleStatisticsImpl() {
    }

    DoubleStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DoubleStatistics dbl = stats.getDoubleStatistics();
      if (dbl.hasMinimum()) {
        hasMinimum = true;
        minimum = dbl.getMinimum();
      }
      if (dbl.hasMaximum()) {
        maximum = dbl.getMaximum();
      }
      if (dbl.hasSum()) {
        sum = dbl.getSum();
      }
    }

    @Override
    public double getMinimum() {
      return minimum;
    }

    @Override
    public double getMaximum() {
      return maximum;
    }

    @Override
    public double getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (hasMinimum) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
      }
      buf.append(" sum: ");
      buf.append(sum);
      return buf.toString();
    }
  }

  protected static final class StringStatisticsImpl extends ColumnStatisticsImpl2
      implements StringColumnStatistics {
    private Text minimum = null;
    private Text maximum = null;
    private long sum = 0;

    StringStatisticsImpl() {
    }

    StringStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.StringStatistics str = stats.getStringStatistics();
      if (str.hasMaximum()) {
        maximum = new Text(str.getMaximum());
      }
      if (str.hasMinimum()) {
        minimum = new Text(str.getMinimum());
      }
      if(str.hasSum()) {
        sum = str.getSum();
      }
    }

    @Override
    public String getMinimum() {
      return minimum == null ? null : minimum.toString();
    }

    @Override
    public String getMaximum() {
      return maximum == null ? null : maximum.toString();
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(getMinimum());
        buf.append(" max: ");
        buf.append(getMaximum());
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }
  }

  protected static final class BinaryStatisticsImpl extends ColumnStatisticsImpl2 implements
      BinaryColumnStatistics {

    private long sum = 0;

    BinaryStatisticsImpl() {
    }

    BinaryStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.BinaryStatistics binStats = stats.getBinaryStatistics();
      if (binStats.hasSum()) {
        sum = binStats.getSum();
      }
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }
  }

  private static final class DecimalStatisticsImpl extends ColumnStatisticsImpl2
      implements DecimalColumnStatistics {
    private HiveDecimal minimum = null;
    private HiveDecimal maximum = null;
    private HiveDecimal sum = HiveDecimal.ZERO;

    DecimalStatisticsImpl() {
    }

    DecimalStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DecimalStatistics dec = stats.getDecimalStatistics();
      if (dec.hasMaximum()) {
        maximum = HiveDecimal.create(dec.getMaximum());
      }
      if (dec.hasMinimum()) {
        minimum = HiveDecimal.create(dec.getMinimum());
      }
      if (dec.hasSum()) {
        sum = HiveDecimal.create(dec.getSum());
      } else {
        sum = null;
      }
    }

    @Override
    public HiveDecimal getMinimum() {
      return minimum;
    }

    @Override
    public HiveDecimal getMaximum() {
      return maximum;
    }

    @Override
    public HiveDecimal getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
        if (sum != null) {
          buf.append(" sum: ");
          buf.append(sum);
        }
      }
      return buf.toString();
    }
  }

  private static final class DateStatisticsImpl extends ColumnStatisticsImpl2
      implements DateColumnStatistics {
    private Integer minimum = null;
    private Integer maximum = null;

    DateStatisticsImpl() {
    }

    DateStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DateStatistics dateStats = stats.getDateStatistics();
      // min,max values serialized/deserialized as int (days since epoch)
      if (dateStats.hasMaximum()) {
        maximum = dateStats.getMaximum();
      }
      if (dateStats.hasMinimum()) {
        minimum = dateStats.getMinimum();
      }
    }

    private transient final DateWritable minDate = new DateWritable();
    private transient final DateWritable maxDate = new DateWritable();

    @Override
    public Date getMinimum() {
      minDate.set(minimum);
      return minDate.get();
    }

    @Override
    public Date getMaximum() {
      maxDate.set(maximum);
      return maxDate.get();
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(getMinimum());
        buf.append(" max: ");
        buf.append(getMaximum());
      }
      return buf.toString();
    }
  }

  private static final class TimestampStatisticsImpl extends ColumnStatisticsImpl2
      implements TimestampColumnStatistics {
    private Long minimum = null;
    private Long maximum = null;

    TimestampStatisticsImpl() {
    }

    TimestampStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.TimestampStatistics timestampStats = stats.getTimestampStatistics();
      // min,max values serialized/deserialized as int (milliseconds since epoch)
      if (timestampStats.hasMaximum()) {
        maximum = timestampStats.getMaximum();
      }
      if (timestampStats.hasMinimum()) {
        minimum = timestampStats.getMinimum();
      }
    }

    @Override
    public Timestamp getMinimum() {
      Timestamp minTimestamp = new Timestamp(minimum);
      return minTimestamp;
    }

    @Override
    public Timestamp getMaximum() {
      Timestamp maxTimestamp = new Timestamp(maximum);
      return maxTimestamp;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
      }
      return buf.toString();
    }
  }

  private long count = 0;
  private boolean hasNull = false;

  ColumnStatisticsImpl2(OrcProto.ColumnStatistics stats) {
    if (stats.hasNumberOfValues()) {
      count = stats.getNumberOfValues();
    }

    if (stats.hasHasNull()) {
      hasNull = stats.getHasNull();
    } else {
      hasNull = true;
    }
  }

  ColumnStatisticsImpl2() {
  }

  void increment() {
    count += 1;
  }

  void setNull() {
    hasNull = true;
  }

  boolean isStatsExists() {
    return (count > 0 || hasNull == true);
  }

  @Override
  public long getNumberOfValues() {
    return count;
  }

  @Override
  public boolean hasNull() {
    return hasNull;
  }

  @Override
  public String toString() {
    return "count: " + count + " hasNull: " + hasNull;
  }


  static ColumnStatisticsImpl2 deserialize(OrcProto.ColumnStatistics stats) {
    if (stats.hasBucketStatistics()) {
      return new BooleanStatisticsImpl(stats);
    } else if (stats.hasIntStatistics()) {
      return new IntegerStatisticsImpl(stats);
    } else if (stats.hasDoubleStatistics()) {
      return new DoubleStatisticsImpl(stats);
    } else if (stats.hasStringStatistics()) {
      return new StringStatisticsImpl(stats);
    } else if (stats.hasDecimalStatistics()) {
      return new DecimalStatisticsImpl(stats);
    } else if (stats.hasDateStatistics()) {
      return new DateStatisticsImpl(stats);
    } else if (stats.hasTimestampStatistics()) {
      return new TimestampStatisticsImpl(stats);
    } else if (stats.hasBinaryStatistics()) {
      return new BinaryStatisticsImpl(stats);
    } else {
      return new ColumnStatisticsImpl2(stats);
    }
  }
}
