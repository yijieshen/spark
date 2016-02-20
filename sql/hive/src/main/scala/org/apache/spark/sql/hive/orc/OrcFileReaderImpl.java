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

import com.google.common.collect.Lists;
import com.google.protobuf.CodedInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.common.DiskRangeList;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OrcFileReaderImpl implements OrcFileReader {

  private static final Log LOG = LogFactory.getLog(OrcFileReaderImpl.class);

  private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;

  protected final FileSystem fileSystem;
  protected final Path path;
  protected final CompressionKind compressionKind;
  protected final CompressionCodec codec;
  protected final int bufferSize;
  private OrcProto.Metadata metadata = null;
  private final int metadataSize;
  protected final OrcProto.Footer footer;
  private final DataType dataType;
  private long deserializedSize = -1;
  protected final Configuration conf;
  private final List<Integer> versionList;
  private final OrcFile.WriterVersion writerVersion;

  //serialized footer - Keeping this around for use by getFileMetaInfo()
  // will help avoid cpu cycles spend in deserializing at cost of increased
  // memory footprint.
  private final ByteBuffer footerByteBuffer;

  @Override
  public long getNumberOfRows() {
    return footer.getNumberOfRows();
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public List<String> getMetadataKeys() {
    List<String> result = new ArrayList<String>();
    for(OrcProto.UserMetadataItem item: footer.getMetadataList()) {
      result.add(item.getName());
    }
    return result;
  }

  @Override
  public ByteBuffer getMetadataValue(String key) {
    for(OrcProto.UserMetadataItem item: footer.getMetadataList()) {
      if (item.hasName() && item.getName().equals(key)) {
        return item.getValue().asReadOnlyByteBuffer();
      }
    }
    throw new IllegalArgumentException("Can't find user metadata " + key);
  }

  @Override
  public boolean hasMetadataValue(String key) {
    for(OrcProto.UserMetadataItem item: footer.getMetadataList()) {
      if (item.hasName() && item.getName().equals(key)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public CompressionKind getCompression() {
    return compressionKind;
  }

  @Override
  public int getCompressionSize() {
    return bufferSize;
  }

  @Override
  public List<StripeInformation> getStripes() {
    List<StripeInformation> result = new ArrayList<StripeInformation>();
    for(OrcProto.StripeInformation info: footer.getStripesList()) {
      result.add(new StripeInformationImpl(info));
    }
    return result;
  }

  @Override
  public long getContentLength() {
    return footer.getContentLength();
  }

  @Override
  public List<OrcProto.Type> getTypes() {
    return footer.getTypesList();
  }

  @Override
  public OrcFile.Version getFileVersion() {
    for (OrcFile.Version version: OrcFile.Version.values()) {
      if (version.getMajor() == versionList.get(0) &&
          version.getMinor() == versionList.get(1)) {
        return version;
      }
    }
    return OrcFile.Version.V_0_11;
  }

  @Override
  public OrcFile.WriterVersion getWriterVersion() {
    return writerVersion;
  }

  @Override
  public int getRowIndexStride() {
    return footer.getRowIndexStride();
  }

  @Override
  public ColumnStatistics[] getStatistics() {
    ColumnStatistics[] result = new ColumnStatistics[footer.getTypesCount()];
    for(int i=0; i < result.length; ++i) {
      result[i] = ColumnStatisticsImpl2.deserialize(footer.getStatistics(i));
    }
    return result;
  }

  @Override
  public Metadata getMetadata() throws IOException {
    return new Metadata(metadata);
  }

  @Override
  public OrcRowBatchReader getRowBatchReader(Options options) throws IOException {
    boolean[] include = options.getInclude();
    if (include == null) {
      include = new boolean[footer.getTypesCount()];
      Arrays.fill(include, true);
      options.include(include);
    }
    return new OrcRowBatchReaderImpl(this.getStripes(), fileSystem, path,
      options, footer.getTypesList(), codec, bufferSize, footer.getRowIndexStride(), conf);
  }

  public OrcFileReaderImpl(Path path, ReaderOptions options) throws IOException {
    FileSystem fs = options.getFilesystem();
    if (fs == null) {
      fs = path.getFileSystem(options.getConfiguration());
    }
    this.fileSystem = fs;
    this.path = path;
    this.conf = options.getConfiguration();

    FileMetaInfo footerMetaData;
    if (options.getFileMetaInfo() != null) {
      footerMetaData = options.getFileMetaInfo();
    } else {
      footerMetaData = extractMetaInfoFromFooter(fs, path, options.getMaxLength());
    }
    MetaInfoObjExtractor rInfo = new MetaInfoObjExtractor(
      footerMetaData.compressionType,
      footerMetaData.bufferSize,
      footerMetaData.metadataSize,
      footerMetaData.footerBuffer);
    this.footerByteBuffer = footerMetaData.footerBuffer;
    this.compressionKind = rInfo.compressionKind;
    this.codec = rInfo.codec;
    this.bufferSize = rInfo.bufferSize;
    this.metadataSize = rInfo.metadataSize;
    this.metadata = rInfo.metadata;
    this.footer = rInfo.footer;
    this.dataType = rInfo.dataType;
    this.versionList = footerMetaData.versionList;
    this.writerVersion = footerMetaData.writerVersion;
  }


  ///////////////////////////////////////////////////////////////////////////
  // File Metadata reader.
  ///////////////////////////////////////////////////////////////////////////

  private static FileMetaInfo
      extractMetaInfoFromFooter(FileSystem fs, Path path, long maxFileLength) throws IOException {

    FSDataInputStream file = fs.open(path);

    // figure out the size of the file using the option or filesystem
    long size;
    if (maxFileLength == Long.MAX_VALUE) {
      size = fs.getFileStatus(path).getLen();
    } else {
      size = maxFileLength;
    }

    //read last bytes into buffer to get PostScript
    int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
    file.seek(size - readSize);
    ByteBuffer buffer = ByteBuffer.allocate(readSize);
    file.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(),
        buffer.remaining());

    //read the PostScript
    //get length of PostScript
    int psLen = buffer.get(readSize - 1) & 0xff;
    ensureOrcFooter(file, path, psLen, buffer);
    int psOffset = readSize - 1 - psLen;
    CodedInputStream in = CodedInputStream.newInstance(buffer.array(),
        buffer.arrayOffset() + psOffset, psLen);
    OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(in);

    checkOrcVersion(LOG, path, ps.getVersionList());

    int footerSize = (int) ps.getFooterLength();
    int metadataSize = (int) ps.getMetadataLength();
    OrcFile.WriterVersion writerVersion;
    if (ps.hasWriterVersion()) {
      writerVersion =  getWriterVersion(ps.getWriterVersion());
    } else {
      writerVersion = OrcFile.WriterVersion.ORIGINAL;
    }

    //check compression codec
    switch (ps.getCompression()) {
      case NONE:
        break;
      case ZLIB:
        break;
      case SNAPPY:
        break;
      case LZO:
        break;
      default:
        throw new IllegalArgumentException("Unknown compression");
    }

    //check if extra bytes need to be read
    int extra = Math.max(0, psLen + 1 + footerSize + metadataSize - readSize);
    if (extra > 0) {
      //more bytes need to be read, seek back to the right place and read extra bytes
      file.seek(size - readSize - extra);
      ByteBuffer extraBuf = ByteBuffer.allocate(extra + readSize);
      file.readFully(extraBuf.array(),
          extraBuf.arrayOffset() + extraBuf.position(), extra);
      extraBuf.position(extra);
      //append with already read bytes
      extraBuf.put(buffer);
      buffer = extraBuf;
      buffer.position(0);
      buffer.limit(footerSize + metadataSize);
    } else {
      //footer is already in the bytes in buffer, just adjust position, length
      buffer.position(psOffset - footerSize - metadataSize);
      buffer.limit(psOffset);
    }

    // remember position for later
    buffer.mark();

    file.close();

    return new FileMetaInfo(
        ps.getCompression().toString(),
        (int) ps.getCompressionBlockSize(),
        (int) ps.getMetadataLength(),
        buffer,
        ps.getVersionList(),
        writerVersion
    );
  }


  /**
   * Ensure this is an ORC file to prevent users from trying to read text
   * files or RC files as ORC files.
   * @param in the file being read
   * @param path the filename for error messages
   * @param psLen the postscript length
   * @param buffer the tail of the file
   * @throws IOException
   */
  static void ensureOrcFooter(
      FSDataInputStream in,
      Path path,
      int psLen,
      ByteBuffer buffer) throws IOException {
    int len = OrcFile.MAGIC.length();
    if (psLen < len + 1) {
      throw new IOException("Malformed ORC file " + path +
          ". Invalid postscript length " + psLen);
    }
    int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - 1
        - len;
    byte[] array = buffer.array();
    // now look for the magic string at the end of the postscript.
    if (!Text.decode(array, offset, len).equals(OrcFile.MAGIC)) {
      // If it isn't there, this may be the 0.11.0 version of ORC.
      // Read the first 3 bytes of the file to check for the header
      in.seek(0);
      byte[] header = new byte[len];
      in.readFully(header, 0, len);
      // if it isn't there, this isn't an ORC file
      if (!Text.decode(header, 0 , len).equals(OrcFile.MAGIC)) {
        throw new IOException("Malformed ORC file " + path +
            ". Invalid postscript.");
      }
    }
  }

  /**
   * Build a version string out of an array.
   * @param version the version number as a list
   * @return the human readable form of the version string
   */
  private static String versionString(List<Integer> version) {
    StringBuilder buffer = new StringBuilder();
    for(int i=0; i < version.size(); ++i) {
      if (i != 0) {
        buffer.append('.');
      }
      buffer.append(version.get(i));
    }
    return buffer.toString();
  }

  /**
   * Check to see if this ORC file is from a future version and if so,
   * warn the user that we may not be able to read all of the column encodings.
   * @param log the logger to write any error message to
   * @param path the filename for error messages
   * @param version the version of hive that wrote the file.
   */
  static void checkOrcVersion(Log log, Path path, List<Integer> version) {
    if (version.size() >= 1) {
      int major = version.get(0);
      int minor = 0;
      if (version.size() >= 2) {
        minor = version.get(1);
      }
      if (major > OrcFile.Version.CURRENT.getMajor() ||
          (major == OrcFile.Version.CURRENT.getMajor() &&
              minor > OrcFile.Version.CURRENT.getMinor())) {
        log.warn("ORC file " + path +
            " was written by a future Hive version " +
            versionString(version) +
            ". This file may not be readable by this version of Hive.");
      }
    }
  }

  static OrcFile.WriterVersion getWriterVersion(int writerVersion) {
    for(OrcFile.WriterVersion version: OrcFile.WriterVersion.values()) {
      if (version.getId() == writerVersion) {
        return version;
      }
    }
    return OrcFile.WriterVersion.ORIGINAL;
  }

  /**
   * FileMetaInfo - represents file metadata stored in footer and postscript sections of the file
   * that is useful for Reader implementation
   *
   */
  static class FileMetaInfo{
    final String compressionType;
    final int bufferSize;
    final int metadataSize;
    final ByteBuffer footerBuffer;
    final List<Integer> versionList;
    final OrcFile.WriterVersion writerVersion;

    FileMetaInfo(String compressionType, int bufferSize, int metadataSize,
                 ByteBuffer footerBuffer, OrcFile.WriterVersion writerVersion) {
      this(compressionType, bufferSize, metadataSize, footerBuffer, null,
          writerVersion);
    }

    FileMetaInfo(String compressionType, int bufferSize, int metadataSize,
                 ByteBuffer footerBuffer, List<Integer> versionList,
                 OrcFile.WriterVersion writerVersion){
      this.compressionType = compressionType;
      this.bufferSize = bufferSize;
      this.metadataSize = metadataSize;
      this.footerBuffer = footerBuffer;
      this.versionList = versionList;
      this.writerVersion = writerVersion;
    }
  }

  public FileMetaInfo getFileMetaInfo(){
    return new FileMetaInfo(compressionKind.toString(), bufferSize,
        metadataSize, footerByteBuffer, versionList, writerVersion);
  }

  public static class ReaderOptions {
    private final Configuration conf;
    private FileSystem filesystem;
    private FileMetaInfo fileMetaInfo;
    private long maxLength = Long.MAX_VALUE;

    public ReaderOptions(Configuration conf) {
      this.conf = conf;
    }
    ReaderOptions fileMetaInfo(FileMetaInfo info) {
      fileMetaInfo = info;
      return this;
    }

    public ReaderOptions filesystem(FileSystem fs) {
      this.filesystem = fs;
      return this;
    }

    public ReaderOptions maxLength(long val) {
      maxLength = val;
      return this;
    }

    Configuration getConfiguration() {
      return conf;
    }

    FileSystem getFilesystem() {
      return filesystem;
    }

    FileMetaInfo getFileMetaInfo() {
      return fileMetaInfo;
    }

    long getMaxLength() {
      return maxLength;
    }
  }

  private static class MetaInfoObjExtractor {
    final CompressionKind compressionKind;
    final CompressionCodec codec;
    final int bufferSize;
    final int metadataSize;
    final OrcProto.Metadata metadata;
    final OrcProto.Footer footer;
    final DataType dataType;

    MetaInfoObjExtractor(
        String codecStr,
        int bufferSize,
        int metadataSize,
        ByteBuffer footerBuffer) throws IOException {

      this.compressionKind = CompressionKind.valueOf(codecStr);
      this.bufferSize = bufferSize;
      this.codec = WriterImpl.createCodec(compressionKind);
      this.metadataSize = metadataSize;

      int position = footerBuffer.position();
      int footerBufferSize = footerBuffer.limit() - footerBuffer.position() - metadataSize;
      footerBuffer.limit(position + metadataSize);

      InputStream instream = InStream.create("metadata", Lists.<DiskRange>newArrayList(
          new BufferChunk(footerBuffer, 0)), metadataSize, codec, bufferSize);
      this.metadata = OrcProto.Metadata.parseFrom(instream);

      footerBuffer.position(position + metadataSize);
      footerBuffer.limit(position + metadataSize + footerBufferSize);
      instream = InStream.create("footer", Lists.<DiskRange>newArrayList(
          new BufferChunk(footerBuffer, 0)), footerBufferSize, codec, bufferSize);
      this.footer = OrcProto.Footer.parseFrom(instream);

      footerBuffer.position(position);
      this.dataType = createDataType(0, footer.getTypesList());
    }
  }

  public static DataType createDataType(int columnId, List<OrcProto.Type> types) {
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case INT:
        return IntegerType$.MODULE$;
      case LONG:
        return LongType$.MODULE$;
      case STRING:
        return StringType$.MODULE$;
      case DOUBLE:
        return DoubleType$.MODULE$;
      case STRUCT:
        return createStructType(columnId, types);
      default:
        throw new UnsupportedOperationException("Unsupported type " +
          type.getKind() +" for vectorized ORC input format");
    }
  }

  public static DataType createStructType(int columnId, List<OrcProto.Type> types) {
    OrcProto.Type type = types.get(columnId);
    int fieldCount = type.getSubtypesCount();
    List<StructField> fields = new ArrayList<>(fieldCount);
    for (int i = 0; i < fieldCount; i ++) {
      int fieldType = type.getSubtypes(i);
      fields.add(
        new StructField(
          type.getFieldNames(i),
          createDataType(fieldType, types),
          true,
          org.apache.spark.sql.types.Metadata.empty()));
    }
    return StructType$.MODULE$.apply(fields);
  }

  public static class BufferChunk extends DiskRangeList {
    final ByteBuffer chunk;

    BufferChunk(ByteBuffer chunk, long offset) {
      super(offset, offset + chunk.remaining());
      this.chunk = chunk;
    }

    @Override
    public boolean hasData() {
      return chunk != null;
    }

    @Override
    public final String toString() {
      boolean makesSense = chunk.remaining() == (end - offset);
      return "data range [" + offset + ", " + end + "), size: " + chunk.remaining()
          + (makesSense ? "" : "(!)") + " type: " + (chunk.isDirect() ? "direct" : "array-backed");
    }

    @Override
    public DiskRange sliceAndShift(long offset, long end, long shiftBy) {
      assert offset <= end && offset >= this.offset && end <= this.end;
      assert offset + shiftBy >= 0;
      ByteBuffer sliceBuf = chunk.slice();
      int newPos = (int)(offset - this.offset);
      int newLimit = newPos + (int)(end - offset);
      try {
        sliceBuf.position(newPos);
        sliceBuf.limit(newLimit);
      } catch (Throwable t) {
        LOG.error("Failed to slice buffer chunk with range" + " [" + this.offset + ", " + this.end
            + "), position: " + chunk.position() + " limit: " + chunk.limit() + ", "
            + (chunk.isDirect() ? "direct" : "array") + "; to [" + offset + ", " + end + ") "
            + t.getClass());
        throw new RuntimeException(t);
      }
      return new BufferChunk(sliceBuf, offset + shiftBy);
    }

    @Override
    public ByteBuffer getData() {
      return chunk;
    }
  }

  static class StripeInformationImpl
      implements StripeInformation {
    private final OrcProto.StripeInformation stripe;

    StripeInformationImpl(OrcProto.StripeInformation stripe) {
      this.stripe = stripe;
    }

    @Override
    public long getOffset() {
      return stripe.getOffset();
    }

    @Override
    public long getLength() {
      return stripe.getDataLength() + getIndexLength() + getFooterLength();
    }

    @Override
    public long getDataLength() {
      return stripe.getDataLength();
    }

    @Override
    public long getFooterLength() {
      return stripe.getFooterLength();
    }

    @Override
    public long getIndexLength() {
      return stripe.getIndexLength();
    }

    @Override
    public long getNumberOfRows() {
      return stripe.getNumberOfRows();
    }

    @Override
    public String toString() {
      return "offset: " + getOffset() + " data: " + getDataLength() +
          " rows: " + getNumberOfRows() + " tail: " + getFooterLength() +
          " index: " + getIndexLength();
    }
  }
}
