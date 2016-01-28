package org.apache.spark.sql.hive.orc;

import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.spark.sql.catalyst.vector.ColumnVector;
import org.apache.spark.sql.catalyst.vector.RowBatch;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TreeReaderFactory {
  protected abstract static class TreeReader {
    protected final int columnId;
    protected BitFieldReader present = null;
    protected boolean valuePresent = false;

    TreeReader(int columnId) throws IOException {
      this(columnId, null);
    }

    TreeReader(int columnId, InStream in) throws IOException {
      this.columnId = columnId;
      if (in == null) {
        present = null;
        valuePresent = true;
      } else {
        present = new BitFieldReader(in, 1);
      }
    }

    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    IntegerReader createIntegerReader(
        OrcProto.ColumnEncoding.Kind kind,
        InStream in,
        boolean signed, boolean skipCorrupt) throws IOException {
      switch (kind) {
        case DIRECT_V2:
        case DICTIONARY_V2:
          return new RunLengthIntegerReaderV2(in, signed, skipCorrupt);
        case DIRECT:
        case DICTIONARY:
          return new RunLengthIntegerReader(in, signed);
        default:
          throw new IllegalArgumentException("Unknown encoding " + kind);
      }
    }

    void startStripe(
        Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      checkEncoding(stripeFooter.getColumnsList().get(columnId));
      InStream in = streams.get(new StreamName(columnId,
          OrcProto.Stream.Kind.PRESENT));
      if (in == null) {
        present = null;
        valuePresent = true;
      } else {
        present = new BitFieldReader(in, 1);
      }
    }

    /**
     * Seek to the given position.
     *
     * @param index the indexes loaded from the file
     * @throws IOException
     */
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    public void seek(PositionProvider index) throws IOException {
      if (present != null) {
        present.seek(index);
      }
    }

    protected long countNonNulls(long rows) throws IOException {
      if (present != null) {
        long result = 0;
        for (long c = 0; c < rows; ++c) {
          if (present.next() == 1) {
            result += 1;
          }
        }
        return result;
      } else {
        return rows;
      }
    }

    abstract void skipRows(long rows) throws IOException;

    /**
     * Populates the isNull vector array in the previousVector object based on
     * the present stream values. This function is called from all the child
     * readers, and they all set the values based on isNull field value.
     *
     * @param previousVector The columnVector object whose isNull value is populated
     * @param batchSize      Size of the column vector
     * @return next column vector
     * @throws IOException
     */
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      ColumnVector result = (ColumnVector) previousVector;
      if (present != null) {
        // Set noNulls and isNull vector of the ColumnVector based on
        // present stream
        result.noNulls = true;
        for (int i = 0; i < batchSize; i++) {
          result.isNull[i] = (present.next() != 1);
          if (result.noNulls && result.isNull[i]) {
            result.noNulls = false;
          }
        }
      } else {
        // There is not present stream, this means that all the values are
        // present.
        result.noNulls = true;
        for (int i = 0; i < batchSize; i++) {
          result.isNull[i] = false;
        }
      }
      return previousVector;
    }
  }

  protected static class IntTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    IntTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null);
    }

    IntTreeReader(int columnId, InStream present, InStream data,
                  OrcProto.ColumnEncoding encoding)
        throws IOException {
      super(columnId, present);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.reader = createIntegerReader(encoding.getKind(), data, true, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(name), true, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final ColumnVector result;
      if (previousVector == null) {
        result = ColumnVector.genIntegerColumnVector(RowBatch.DEFAULT_SIZE);
      } else {
        result = (ColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextIntVector(result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  protected static class LongTreeReader extends TreeReader {
    protected IntegerReader reader = null;

    LongTreeReader(int columnId, boolean skipCorrupt) throws IOException {
      this(columnId, null, null, null, skipCorrupt);
    }

    LongTreeReader(int columnId, InStream present, InStream data,
                   OrcProto.ColumnEncoding encoding,
                   boolean skipCorrupt)
        throws IOException {
      super(columnId, present);
      if (data != null && encoding != null) {
        checkEncoding(encoding);
        this.reader = createIntegerReader(encoding.getKind(), data, true, skipCorrupt);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if ((encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT) &&
          (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2)) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(
        Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter) throws IOException {
      super.startStripe(streams, stripeFooter);
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(name), true, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final ColumnVector result;
      if (previousVector == null) {
        result = ColumnVector.genLongColumnVector(RowBatch.DEFAULT_SIZE);
      } else {
        result = (ColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      // Read value entries based on isNull entries
      reader.nextLongVector(result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  protected static class DoubleTreeReader extends TreeReader {
    protected InStream stream;
    private final SerializationUtils utils;

    DoubleTreeReader(int columnId) throws IOException {
      this(columnId, null, null);
    }

    DoubleTreeReader(int columnId, InStream present, InStream data) throws IOException {
      super(columnId, present);
      this.utils = new SerializationUtils();
      this.stream = data;
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      StreamName name =
          new StreamName(columnId,
              OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      stream.seek(index);
    }

    @Override
    public Object nextVector(Object previousVector, final long batchSize) throws IOException {
      final ColumnVector result;
      if (previousVector == null) {
        result = ColumnVector.genDoubleColumnVector(RowBatch.DEFAULT_SIZE);
      } else {
        result = (ColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      final boolean hasNulls = !result.noNulls;
      boolean allNulls = hasNulls;

      if (hasNulls) {
        // conditions to ensure bounds checks skips
        for (int i = 0; i < batchSize && batchSize <= result.isNull.length; i++) {
          allNulls = allNulls & result.isNull[i];
        }
        if (allNulls) {
          result.doubleVector[0] = Double.NaN;
          result.isRepeating = true;
        } else {
          // some nulls
          result.isRepeating = false;
          // conditions to ensure bounds checks skips
          for (int i = 0; batchSize <= result.isNull.length
              && batchSize <= result.doubleVector.length && i < batchSize; i++) {
            if (!result.isNull[i]) {
              result.doubleVector[i] = utils.readDouble(stream);
            } else {
              // If the value is not present then set NaN
              result.doubleVector[i] = Double.NaN;
            }
          }
        }
      } else {
        // no nulls
        boolean repeating = (batchSize > 1);
        final double d1 = utils.readDouble(stream);
        result.doubleVector[0] = d1;
        // conditions to ensure bounds checks skips
        for (int i = 1; i < batchSize && batchSize <= result.doubleVector.length; i++) {
          final double d2 = utils.readDouble(stream);
          repeating = repeating && (d1 == d2);
          result.doubleVector[i] = d2;
        }
        result.isRepeating = repeating;
      }

      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long len = items * 8;
      while (len > 0) {
        len -= stream.skip(len);
      }
    }
  }

  protected static class StringTreeReader extends TreeReader {
    protected TreeReader reader;

    StringTreeReader(int columnId) throws IOException {
      super(columnId);
    }

    StringTreeReader(
        int columnId, InStream present, InStream data, InStream length,
        InStream dictionary, OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present);
      if (encoding != null) {
        switch (encoding.getKind()) {
          case DIRECT:
          case DIRECT_V2:
            reader = new StringDirectTreeReader(columnId, present, data, length,
                encoding.getKind());
            break;
          case DICTIONARY:
          case DICTIONARY_V2:
            reader = new StringDictionaryTreeReader(columnId, present, data, length, dictionary,
                encoding);
            break;
          default:
            throw new IllegalArgumentException("Unsupported encoding " +
                encoding.getKind());
        }
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      reader.checkEncoding(encoding);
    }

    @Override
    void startStripe(
        Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter) throws IOException {
      // For each stripe, checks the encoding and initializes the appropriate
      // reader
      switch (stripeFooter.getColumnsList().get(columnId).getKind()) {
        case DIRECT:
        case DIRECT_V2:
          reader = new StringDirectTreeReader(columnId);
          break;
        case DICTIONARY:
        case DICTIONARY_V2:
          reader = new StringDictionaryTreeReader(columnId);
          break;
        default:
          throw new IllegalArgumentException("Unsupported encoding " +
            stripeFooter.getColumnsList().get(columnId).getKind());
      }
      reader.startStripe(streams, stripeFooter);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      reader.seek(index);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      reader.seek(index);
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      return reader.nextVector(previousVector, batchSize);
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skipRows(items);
    }
  }

  /**
   * A reader for string columns that are direct encoded in the current
   * stripe.
   */
  protected static class StringDirectTreeReader extends TreeReader {
    protected InStream stream;
    protected IntegerReader lengths;
    private final ColumnVector scratchlcv;

    StringDirectTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null, null);
    }

    StringDirectTreeReader(
        int columnId, InStream present, InStream data, InStream length,
        OrcProto.ColumnEncoding.Kind encoding) throws IOException {
      super(columnId, present);
      this.scratchlcv = ColumnVector.genIntegerColumnVector(RowBatch.DEFAULT_SIZE);
      this.stream = data;
      if (length != null && encoding != null) {
        this.lengths = createIntegerReader(encoding, length, false, false);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT &&
          encoding.getKind() != OrcProto.ColumnEncoding.Kind.DIRECT_V2) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(Map<StreamName, InStream> streams,
                     OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);
      StreamName name = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
      stream = streams.get(name);
      lengths = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(new StreamName(columnId, OrcProto.Stream.Kind.LENGTH)),
          false, false);
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      stream.seek(index);
      lengths.seek(index);
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final ColumnVector result;
      if (previousVector == null) {
        result = ColumnVector.genStringColumnVector(RowBatch.DEFAULT_SIZE);
      } else {
        result = (ColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      BytesColumnVectorUtil.readOrcByteArrays(stream, lengths, scratchlcv, result, batchSize);
      return result;
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      long lengthToSkip = 0;
      for (int i = 0; i < items; ++i) {
        lengthToSkip += lengths.next();
      }

      while (lengthToSkip > 0) {
        lengthToSkip -= stream.skip(lengthToSkip);
      }
    }
  }

  // This class collects together very similar methods for reading an ORC vector of byte arrays and
  // creating the BytesColumnVector.
  //
  public static class BytesColumnVectorUtil {

    private static byte[] commonReadByteArrays(
        InStream stream, IntegerReader lengths,
        ColumnVector scratchlcv,
        ColumnVector result, long batchSize) throws IOException {
      // Read lengths
      scratchlcv.isNull = result.isNull;  // Notice we are replacing the isNull vector here...
      lengths.nextIntVector(scratchlcv, batchSize);
      int totalLength = 0;
      if (!scratchlcv.isRepeating) {
        for (int i = 0; i < batchSize; i++) {
          if (!scratchlcv.isNull[i]) {
            totalLength += scratchlcv.intVector[i];
          }
        }
      } else {
        if (!scratchlcv.isNull[0]) {
          totalLength = (int) (batchSize * scratchlcv.intVector[0]);
        }
      }

      // Read all the strings for this batch
      byte[] allBytes = new byte[totalLength];
      int offset = 0;
      int len = totalLength;
      while (len > 0) {
        int bytesRead = stream.read(allBytes, offset, len);
        if (bytesRead < 0) {
          throw new EOFException("Can't finish byte read from " + stream);
        }
        len -= bytesRead;
        offset += bytesRead;
      }

      return allBytes;
    }

    // This method has the common code for reading in bytes into a BytesColumnVector.
    public static void readOrcByteArrays(
        InStream stream, IntegerReader lengths,
        ColumnVector scratchlcv,
        ColumnVector result, long batchSize) throws IOException {

      byte[] allBytes = commonReadByteArrays(stream, lengths, scratchlcv, result, batchSize);

      // Too expensive to figure out 'repeating' by comparisons.
      result.isRepeating = false;
      int offset = 0;
      if (!scratchlcv.isRepeating) {
        for (int i = 0; i < batchSize; i++) {
          if (!scratchlcv.isNull[i]) {
            result.setRef(i, allBytes, offset, scratchlcv.intVector[i]);
            offset += scratchlcv.intVector[i];
          } else {
            result.setRef(i, allBytes, 0, 0);
          }
        }
      } else {
        for (int i = 0; i < batchSize; i++) {
          if (!scratchlcv.isNull[i]) {
            result.setRef(i, allBytes, offset, scratchlcv.intVector[0]);
            offset += scratchlcv.intVector[0];
          } else {
            result.setRef(i, allBytes, 0, 0);
          }
        }
      }
    }
  }

  /**
   * A reader for string columns that are dictionary encoded in the current
   * stripe.
   */
  protected static class StringDictionaryTreeReader extends TreeReader {
    private DynamicByteArray dictionaryBuffer;
    private int[] dictionaryOffsets;
    protected IntegerReader reader;

    private byte[] dictionaryBufferInBytesCache = null;
    private final ColumnVector scratchlcv;

    StringDictionaryTreeReader(int columnId) throws IOException {
      this(columnId, null, null, null, null, null);
    }

    StringDictionaryTreeReader(
        int columnId, InStream present, InStream data,
        InStream length, InStream dictionary,
        OrcProto.ColumnEncoding encoding) throws IOException {
      super(columnId, present);
      scratchlcv = ColumnVector.genIntegerColumnVector(RowBatch.DEFAULT_SIZE);
      if (data != null && encoding != null) {
        this.reader = createIntegerReader(encoding.getKind(), data, false, false);
      }

      if (dictionary != null && encoding != null) {
        readDictionaryStream(dictionary);
      }

      if (length != null && encoding != null) {
        readDictionaryLengthStream(length, encoding);
      }
    }

    @Override
    void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
      if (encoding.getKind() != OrcProto.ColumnEncoding.Kind.DICTIONARY &&
          encoding.getKind() != OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
        throw new IOException("Unknown encoding " + encoding + " in column " +
            columnId);
      }
    }

    @Override
    void startStripe(
        Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter
    ) throws IOException {
      super.startStripe(streams, stripeFooter);

      // read the dictionary blob
      StreamName name = new StreamName(columnId,
          OrcProto.Stream.Kind.DICTIONARY_DATA);
      InStream in = streams.get(name);
      readDictionaryStream(in);

      // read the lengths
      name = new StreamName(columnId, OrcProto.Stream.Kind.LENGTH);
      in = streams.get(name);
      readDictionaryLengthStream(in, stripeFooter.getColumnsList().get(columnId));

      // set up the row reader
      name = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
      reader = createIntegerReader(stripeFooter.getColumnsList().get(columnId).getKind(),
          streams.get(name), false, false);
    }

    private void readDictionaryLengthStream(InStream in, OrcProto.ColumnEncoding encoding)
        throws IOException {
      int dictionarySize = encoding.getDictionarySize();
      if (in != null) { // Guard against empty LENGTH stream.
        IntegerReader lenReader = createIntegerReader(encoding.getKind(), in, false, false);
        int offset = 0;
        if (dictionaryOffsets == null ||
            dictionaryOffsets.length < dictionarySize + 1) {
          dictionaryOffsets = new int[dictionarySize + 1];
        }
        for (int i = 0; i < dictionarySize; ++i) {
          dictionaryOffsets[i] = offset;
          offset += (int) lenReader.next();
        }
        dictionaryOffsets[dictionarySize] = offset;
        in.close();
      }

    }

    private void readDictionaryStream(InStream in) throws IOException {
      if (in != null) { // Guard against empty dictionary stream.
        if (in.available() > 0) {
          dictionaryBuffer = new DynamicByteArray(64, in.available());
          dictionaryBuffer.readAll(in);
          // Since its start of strip invalidate the cache.
          dictionaryBufferInBytesCache = null;
        }
        in.close();
      } else {
        dictionaryBuffer = null;
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      seek(index[columnId]);
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      super.seek(index);
      reader.seek(index);
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final ColumnVector result;
      int offset;
      int length;
      if (previousVector == null) {
        result = ColumnVector.genStringColumnVector(RowBatch.DEFAULT_SIZE);
      } else {
        result = (ColumnVector) previousVector;
      }

      // Read present/isNull stream
      super.nextVector(result, batchSize);

      if (dictionaryBuffer != null) {

        // Load dictionaryBuffer into cache.
        if (dictionaryBufferInBytesCache == null) {
          dictionaryBufferInBytesCache = dictionaryBuffer.get();
        }

        // Read string offsets
        scratchlcv.isNull = result.isNull;
        reader.nextIntVector(scratchlcv, batchSize);
        if (!scratchlcv.isRepeating) {

          // The vector has non-repeating strings. Iterate thru the batch
          // and set strings one by one
          for (int i = 0; i < batchSize; i++) {
            if (!scratchlcv.isNull[i]) {
              offset = dictionaryOffsets[scratchlcv.intVector[i]];
              length = getDictionaryEntryLength(scratchlcv.intVector[i], offset);
              result.setRef(i, dictionaryBufferInBytesCache, offset, length);
            } else {
              // If the value is null then set offset and length to zero (null string)
              result.setRef(i, dictionaryBufferInBytesCache, 0, 0);
            }
          }
        } else {
          // If the value is repeating then just set the first value in the
          // vector and set the isRepeating flag to true. No need to iterate thru and
          // set all the elements to the same value
          offset = dictionaryOffsets[scratchlcv.intVector[0]];
          length = getDictionaryEntryLength(scratchlcv.intVector[0], offset);
          result.setRef(0, dictionaryBufferInBytesCache, offset, length);
        }
        result.isRepeating = scratchlcv.isRepeating;
      } else {
        // Entire stripe contains null strings.
        result.isRepeating = true;
        result.noNulls = false;
        result.isNull[0] = true;
        result.setRef(0, "".getBytes(), 0, 0);
      }
      return result;
    }

    int getDictionaryEntryLength(int entry, int offset) {
      final int length;
      // if it isn't the last entry, subtract the offsets otherwise use
      // the buffer length.
      if (entry < dictionaryOffsets.length - 1) {
        length = dictionaryOffsets[entry + 1] - offset;
      } else {
        length = dictionaryBuffer.size() - offset;
      }
      return length;
    }

    @Override
    void skipRows(long items) throws IOException {
      reader.skip(countNonNulls(items));
    }
  }

  // TODO: where included starts from?
  public static List<Integer> getIncluded(boolean[] included) {
    List<Integer> result = new ArrayList<>(included.length);
    for (int i = 1; i < included.length; i ++) {
      if (included[i]) {
        result.add(i);
      }
    }
    return result;
  }

  protected static class StructTreeReader extends TreeReader {
    protected final TreeReader[] fields;
    private final String[] fieldNames;
    private final int requiredColsNum;

    StructTreeReader(
        int columnId,
        List<OrcProto.Type> types,
        boolean[] included,
        boolean skipCorrupt) throws IOException {
      super(columnId);
      OrcProto.Type type = types.get(columnId);
      List<Integer> requiredCols =
        included == null ? null : getIncluded(included);
      int fieldCount = type.getFieldNamesCount();
      requiredColsNum =
        included == null ? fieldCount : requiredCols.size();
      this.fields = new TreeReader[requiredColsNum];
      this.fieldNames = new String[requiredColsNum];
      int j = 0;
      for (int i = 0; i < fieldCount; ++i) {
        int subtype = type.getSubtypes(i);
        if (included == null) {
          this.fields[i] = createTreeReader(subtype, types, included, skipCorrupt);
          this.fieldNames[i] = type.getFieldNames(i);
        } else if (included[subtype]) {
          this.fields[j] = createTreeReader(subtype, types, included, skipCorrupt);
          this.fieldNames[j] = type.getFieldNames(i);
          j += 1;
        }
      }
    }

    @Override
    void seek(PositionProvider[] index) throws IOException {
      super.seek(index);
      for (TreeReader kid : fields) {
        kid.seek(index);
      }
    }

    @Override
    public Object nextVector(Object previousVector, long batchSize) throws IOException {
      final ColumnVector[] result;
      if (previousVector == null) {
        result = new ColumnVector[requiredColsNum];
      } else {
        result = (ColumnVector[]) previousVector;
      }

      // Read all the members of struct as column vectors
      for (int i = 0; i < requiredColsNum; i++) {
        if (result[i] == null) {
          assert false;
          result[i] = (ColumnVector) fields[i].nextVector(null, batchSize);
        } else {
          fields[i].nextVector(result[i], batchSize);
        }
      }
      return result;
    }

    @Override
    void startStripe(
        Map<StreamName, InStream> streams,
        OrcProto.StripeFooter stripeFooter) throws IOException {
      super.startStripe(streams, stripeFooter);
      for (TreeReader field : fields) {
        field.startStripe(streams, stripeFooter);
      }
    }

    @Override
    void skipRows(long items) throws IOException {
      items = countNonNulls(items);
      for (TreeReader field : fields) {
        field.skipRows(items);
      }
    }
  }

  public static TreeReader createTreeReader(
      int columnId,
      List<OrcProto.Type> types,
      boolean[] included,
      boolean skipCorrupt) throws IOException {
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case DOUBLE:
        return new DoubleTreeReader(columnId);
      case INT:
        return new IntTreeReader(columnId);
      case LONG:
        return new LongTreeReader(columnId, skipCorrupt);
      case STRING:
        return new StringTreeReader(columnId);
      case STRUCT:
        return new StructTreeReader(columnId, types, included, skipCorrupt);
      default:
        throw new IllegalArgumentException("Unsupported type " +
          type.getKind());
    }
  }
}
