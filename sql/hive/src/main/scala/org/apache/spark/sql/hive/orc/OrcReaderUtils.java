package org.apache.spark.sql.hive.orc;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.common.DiskRangeList;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class OrcReaderUtils {

  static DiskRangeList readDiskRanges(
      FSDataInputStream file,
      long base,
      DiskRangeList range,
      boolean doForceDirect) throws IOException {
    if (range == null) return null;
    DiskRangeList prev = range.prev;
    if (prev == null) {
      prev = new DiskRangeList.DiskRangeListMutateHelper(range);
    }
    while (range != null) {
      if (range.hasData()) {
        range = range.next;
        continue;
      }
      int len = (int) (range.getEnd() - range.getOffset());
      long off = range.getOffset();
      file.seek(base + off);
      if (doForceDirect) {
        ByteBuffer directBuf = ByteBuffer.allocateDirect(len);
        readDirect(file, len, directBuf);
        range = range.replaceSelfWith(
            new OrcFileReaderImpl.BufferChunk(directBuf, range.getOffset()));
      } else {
        byte[] buffer = new byte[len];
        file.readFully(buffer, 0, buffer.length);
        range = range.replaceSelfWith(
            new OrcFileReaderImpl.BufferChunk(ByteBuffer.wrap(buffer), range.getOffset()));
      }
      range = range.next;
    }
    return prev.next;
  }

  static List<DiskRange> getStreamBuffers(DiskRangeList range, long offset, long length) {
    // This assumes sorted ranges (as do many other parts of ORC code.
    ArrayList<DiskRange> buffers = new ArrayList<DiskRange>();
    if (length == 0) return buffers;
    long streamEnd = offset + length;
    boolean inRange = false;
    while (range != null) {
      if (!inRange) {
        if (range.getEnd() <= offset) {
          range = range.next;
          continue; // Skip until we are in range.
        }
        inRange = true;
        if (range.getOffset() < offset) {
          // Partial first buffer, add a slice of it.
          buffers.add(range.sliceAndShift(offset, Math.min(streamEnd, range.getEnd()), -offset));
          if (range.getEnd() >= streamEnd) break; // Partial first buffer is also partial last buffer.
          range = range.next;
          continue;
        }
      } else if (range.getOffset() >= streamEnd) {
        break;
      }
      if (range.getEnd() > streamEnd) {
        // Partial last buffer (may also be the first buffer), add a slice of it.
        buffers.add(range.sliceAndShift(range.getOffset(), streamEnd, -offset));
        break;
      }
      // Buffer that belongs entirely to one stream.
      // TODO: ideally we would want to reuse the object and remove it from the list, but we cannot
      //       because bufferChunks is also used by clearStreams for zcr. Create a useless dup.
      buffers.add(range.sliceAndShift(range.getOffset(), range.getEnd(), -offset));
      if (range.getEnd() == streamEnd) break;
      range = range.next;
    }
    return buffers;
  }

  public static void readDirect(
      FSDataInputStream file,
      int len, ByteBuffer directBuf) throws IOException {
    // TODO: HDFS API is a mess, so handle all kinds of cases.
    // Before 2.7, read() also doesn't adjust position correctly, so track it separately.
    int pos = directBuf.position(), startPos = pos, endPos = pos + len;
    try {
      while (pos < endPos) {
        int count = readByteBuffer(file, directBuf);
        if (count < 0) throw new EOFException();
        assert count != 0 : "0-length read: " + (endPos - pos) + "@" + (pos - startPos);
        pos += count;
        assert pos <= endPos : "Position " + pos + " > " + endPos + " after reading " + count;
        directBuf.position(pos);
      }
    } catch (UnsupportedOperationException ex) {
      assert pos == startPos;
      // Happens in q files and such.
      OrcRowBatchReaderImpl.LOG.error("Stream does not support direct read; we will copy.");
      byte[] buffer = new byte[len];
      file.readFully(buffer, 0, buffer.length);
      directBuf.put(buffer);
    }
    directBuf.position(startPos);
    directBuf.limit(startPos + len);
  }

  public static int readByteBuffer(FSDataInputStream file, ByteBuffer dest) throws IOException {
    int pos = dest.position();
    int result = file.read(dest);
    if (result > 0) {
      // Ensure this explicitly since versions before 2.7 read doesn't do it.
      dest.position(pos + result);
    }
    return result;
  }

  static boolean[] findPresentStreamsByColumn(
      List<OrcProto.Stream> streamList, List<OrcProto.Type> types) {
    boolean[] hasNull = new boolean[types.size()];
    for(OrcProto.Stream stream: streamList) {
      if (stream.hasKind() && (stream.getKind() == OrcProto.Stream.Kind.PRESENT)) {
        hasNull[stream.getColumn()] = true;
      }
    }
    return hasNull;
  }

  // for uncompressed streams, what is the most overlap with the following set
  // of rows (long vint literal group).
  static final int WORST_UNCOMPRESSED_SLOP = 2 + 8 * 512;
  static final int HEADER_SIZE = 3;

  /**
   * Is this stream part of a dictionary?
   * @return is this part of a dictionary?
   */
  static boolean isDictionary(
      OrcProto.Stream.Kind kind,
      OrcProto.ColumnEncoding encoding) {
    assert kind != OrcProto.Stream.Kind.DICTIONARY_COUNT;
    OrcProto.ColumnEncoding.Kind encodingKind = encoding.getKind();
    return kind == OrcProto.Stream.Kind.DICTIONARY_DATA ||
        (kind == OrcProto.Stream.Kind.LENGTH &&
            (encodingKind == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
                encodingKind == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2));
  }

  static String stringifyDiskRanges(DiskRangeList range) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("[");
    boolean isFirst = true;
    while (range != null) {
      if (!isFirst) {
        buffer.append(", ");
      }
      isFirst = false;
      buffer.append(range.toString());
      range = range.next;
    }
    buffer.append("]");
    return buffer.toString();
  }

  static void addEntireStreamToRanges(
      long offset, long length, DiskRangeList.DiskRangeListCreateHelper list, boolean doMergeBuffers) {
    list.addOrMerge(offset, offset + length, doMergeBuffers, false);
  }

  static void addRgFilteredStreamToRanges(
      OrcProto.Stream stream,
      boolean[] includedRowGroups, boolean isCompressed, OrcProto.RowIndex index,
      OrcProto.ColumnEncoding encoding, OrcProto.Type type, int compressionSize, boolean hasNull,
      long offset, long length, DiskRangeList.DiskRangeListCreateHelper list, boolean doMergeBuffers) {
    for (int group = 0; group < includedRowGroups.length; ++group) {
      if (!includedRowGroups[group]) continue;
      int posn = getIndexPosition(
          encoding.getKind(), type.getKind(), stream.getKind(), isCompressed, hasNull);
      long start = index.getEntry(group).getPositions(posn);
      final long nextGroupOffset;
      boolean isLast = group == (includedRowGroups.length - 1);
      nextGroupOffset = isLast ? length : index.getEntry(group + 1).getPositions(posn);

      start += offset;
      long end = offset + estimateRgEndOffset(
          isCompressed, isLast, nextGroupOffset, length, compressionSize);
      list.addOrMerge(start, end, doMergeBuffers, true);
    }
  }

  static long estimateRgEndOffset(boolean isCompressed, boolean isLast,
                                  long nextGroupOffset, long streamLength, int bufferSize) {
    // figure out the worst case last location
    // if adjacent groups have the same compressed block offset then stretch the slop
    // by factor of 2 to safely accommodate the next compression block.
    // One for the current compression block and another for the next compression block.
    long slop = isCompressed ? 2 * (HEADER_SIZE + bufferSize) : WORST_UNCOMPRESSED_SLOP;
    return isLast ? streamLength : Math.min(streamLength, nextGroupOffset + slop);
  }

  private static final int BYTE_STREAM_POSITIONS = 1;
  private static final int RUN_LENGTH_BYTE_POSITIONS = BYTE_STREAM_POSITIONS + 1;
  private static final int BITFIELD_POSITIONS = RUN_LENGTH_BYTE_POSITIONS + 1;
  private static final int RUN_LENGTH_INT_POSITIONS = BYTE_STREAM_POSITIONS + 1;

  /**
   * Get the offset in the index positions for the column that the given
   * stream starts.
   * @param columnEncoding the encoding of the column
   * @param columnType the type of the column
   * @param streamType the kind of the stream
   * @param isCompressed is the file compressed
   * @param hasNulls does the column have a PRESENT stream?
   * @return the number of positions that will be used for that stream
   */
  public static int getIndexPosition(
      OrcProto.ColumnEncoding.Kind columnEncoding,
      OrcProto.Type.Kind columnType,
      OrcProto.Stream.Kind streamType,
      boolean isCompressed,
      boolean hasNulls) {
    if (streamType == OrcProto.Stream.Kind.PRESENT) {
      return 0;
    }
    int compressionValue = isCompressed ? 1 : 0;
    int base = hasNulls ? (BITFIELD_POSITIONS + compressionValue) : 0;
    switch (columnType) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DATE:
      case STRUCT:
      case MAP:
      case LIST:
      case UNION:
        return base;
      case CHAR:
      case VARCHAR:
      case STRING:
        if (columnEncoding == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
            columnEncoding == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
          return base;
        } else {
          if (streamType == OrcProto.Stream.Kind.DATA) {
            return base;
          } else {
            return base + BYTE_STREAM_POSITIONS + compressionValue;
          }
        }
      case BINARY:
        if (streamType == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + BYTE_STREAM_POSITIONS + compressionValue;
      case DECIMAL:
        if (streamType == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + BYTE_STREAM_POSITIONS + compressionValue;
      case TIMESTAMP:
        if (streamType == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + RUN_LENGTH_INT_POSITIONS + compressionValue;
      default:
        throw new IllegalArgumentException("Unknown type " + columnType);
    }
  }
}
