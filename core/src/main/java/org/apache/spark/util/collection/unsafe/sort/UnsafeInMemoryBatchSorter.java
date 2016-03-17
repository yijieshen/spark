package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.collection.MergeSorter;

import java.util.ArrayList;
import java.util.Comparator;

public final class UnsafeInMemoryBatchSorter {

  private static final class SortComparator implements Comparator<RecordPointerAndKeyPrefix> {

    private final RecordComparator recordComparator;
    private final PrefixComparator prefixComparator;
    private final TaskMemoryManager memoryManager;

    private final boolean needFurtherCompare;

    SortComparator(
        RecordComparator recordComparator,
        PrefixComparator prefixComparator,
        boolean furtherCompare,
        TaskMemoryManager memoryManager) {
      this.recordComparator = recordComparator;
      this.prefixComparator = prefixComparator;
      this.needFurtherCompare = furtherCompare;
      this.memoryManager = memoryManager;
    }

    @Override
    public int compare(RecordPointerAndKeyPrefix r1, RecordPointerAndKeyPrefix r2) {
      final int prefixComparisonResult = prefixComparator.compare(r1.keyPrefix, r2.keyPrefix);
      if (prefixComparisonResult == 0 && needFurtherCompare) {
        final Object baseObject1 = memoryManager.getPage(r1.recordPointer);
        final long baseOffset1 = memoryManager.getOffsetInPage(r1.recordPointer) + 4; // skip length
        final Object baseObject2 = memoryManager.getPage(r2.recordPointer);
        final long baseOffset2 = memoryManager.getOffsetInPage(r2.recordPointer) + 4; // skip length
        return recordComparator.compare(baseObject1, baseOffset1, baseObject2, baseOffset2);
      } else {
        return prefixComparisonResult;
      }
    }
  }

  private final MemoryConsumer consumer;
  private final TaskMemoryManager memoryManager;
  private final MergeSorter<RecordPointerAndKeyPrefix, LongArray> sorter;
  private final Comparator<RecordPointerAndKeyPrefix> sortComparator;

  /**
   * Within this buffer, position {@code 2 * i} holds a pointer pointer to the record at
   * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
   */
  private LongArray array;

  private ArrayList<Integer> startsList;
  private ArrayList<Integer> lengthsList;

  /**
   * The position in the sort buffer where new records can be inserted.
   */
  private int pos = 0;

  private int numRecordsInCurrentBatch = 0;
  private boolean rowBatchStarted = false;

  public UnsafeInMemoryBatchSorter(
      final MemoryConsumer consumer,
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      boolean furtherCompare,
      int initialSize) {
    this(consumer, memoryManager, recordComparator, prefixComparator, furtherCompare,
        consumer.allocateArray(initialSize * 2));
  }

  public UnsafeInMemoryBatchSorter(
      final MemoryConsumer consumer,
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      boolean furtherCompare,
      LongArray array) {
    this.consumer = consumer;
    this.memoryManager = memoryManager;
    this.sorter = new MergeSorter<>(UnsafeSortDataFormat.INSTANCE);
    this.sortComparator =
      new SortComparator(recordComparator, prefixComparator, furtherCompare, memoryManager);
    this.array = array;
    this.startsList = new ArrayList<>();
    this.lengthsList = new ArrayList<>();
  }

  /**
   * Free the memory used by pointer array.
   */
  public void free() {
    consumer.freeArray(array);
    array = null;
    startsList = null;
    lengthsList = null;
  }

  public void reset() {
    pos = 0;
    startsList.clear();
    lengthsList.clear();
    startRowBatch();
  }

  /**
   * @return the number of records that have been inserted into this sorter.
   */
  public int numRecords() {
    return pos / 2;
  }

  public long getMemoryUsage() {
    return array.size() * 8L;
  }

  public boolean hasSpaceForAnotherRecord() {
    return pos + 2 <= array.size();
  }

  public void expandPointerArray(LongArray newArray) {
    if (newArray.size() < array.size()) {
      throw new OutOfMemoryError("Not enough memory to grow pointer array");
    }
    Platform.copyMemory(
        array.getBaseObject(),
        array.getBaseOffset(),
        newArray.getBaseObject(),
        newArray.getBaseOffset(),
        array.size() * 8L);
    consumer.freeArray(array);
    array = newArray;
  }

  public void startRowBatch() {
    assert !rowBatchStarted;
    rowBatchStarted = true;
    startsList.add(pos / 2);
    numRecordsInCurrentBatch = 0;
  }

  public void endRowBatch() {
    assert rowBatchStarted;
    lengthsList.add(numRecordsInCurrentBatch);
    rowBatchStarted = false;
  }

  /**
   * Inserts a record to be sorted. Assumes that the record pointer points to a record length
   * stored as a 4-byte integer, followed by the record's bytes.
   *
   * Note: the caller of this class is responsible for space checking for the next record and
   * expand the space on demand.
   *
   * @param recordPointer pointer to a record in a data page, encoded by {@link TaskMemoryManager}.
   * @param keyPrefix a user-defined key prefix
   */
  public void insertRecord(long recordPointer, long keyPrefix) {
    array.set(pos, recordPointer);
    pos ++;
    array.set(pos, keyPrefix);
    pos ++;
    numRecordsInCurrentBatch ++;
  }

  public final class SortedIterator extends UnsafeSorterIterator {

    private final int numRecords;
    private int position;
    private Object baseObject;
    private long baseOffset;
    private long keyPrefix;
    private int recordLength;

    private SortedIterator(int numRecords) {
      this.numRecords = numRecords;
      this.position = 0;
    }

    public SortedIterator clone() {
      SortedIterator iter = new SortedIterator(numRecords);
      iter.position = position;
      iter.baseObject = baseObject;
      iter.baseOffset = baseOffset;
      iter.keyPrefix = keyPrefix;
      iter.recordLength = recordLength;
      return iter;
    }

    @Override
    public boolean hasNext() {
      return position / 2 < numRecords;
    }

    public int numRecordsLeft() {
      return numRecords - position / 2;
    }

    @Override
    public void loadNext() {
      // This pointer points to a 4-byte record length, followed by the record's bytes
      final long recordPointer = array.get(position);
      baseObject = memoryManager.getPage(recordPointer);
      baseOffset = memoryManager.getOffsetInPage(recordPointer) + 4;  // Skip over record length
      recordLength = Platform.getInt(baseObject, baseOffset - 4);
      keyPrefix = array.get(position + 1);
      position += 2;
    }

    @Override
    public Object getBaseObject() { return baseObject; }

    @Override
    public long getBaseOffset() { return baseOffset; }

    @Override
    public int getRecordLength() { return recordLength; }

    @Override
    public long getKeyPrefix() { return keyPrefix; }
  }

  /**
   * Return an iterator over record pointers in sorted order. For efficiency, all calls to
   * {@code next()} will return the same mutable object.
   */
  public SortedIterator getSortedIterator() {
    if (rowBatchStarted) {
      endRowBatch();
    }
    assert startsList.size() == lengthsList.size();
    sorter.sort(array, 0, pos / 2, sortComparator,
      startsList.toArray(new Integer[0]), lengthsList.toArray(new Integer[0]));
    return new SortedIterator(pos / 2);
  }

  /**
   * Returns an iterator over record pointers in original order (inserted).
   */
  public SortedIterator getIterator() {
    return new SortedIterator(pos / 2);
  }
}
