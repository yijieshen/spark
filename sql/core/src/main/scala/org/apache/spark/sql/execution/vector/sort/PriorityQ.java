package org.apache.spark.sql.execution.vector.sort;

import org.apache.spark.sql.catalyst.expressions.vector.InterBatchOrdering;
import org.apache.spark.sql.catalyst.vector.RowBatch;

public class PriorityQ {

  private RowBatch[] queue;
  private int size = 0;
  private InterBatchOrdering interOrder;

  public PriorityQ(InterBatchOrdering interBatchOrdering) {
    this.interOrder = interBatchOrdering;
  }

  private int compare(RowBatch l, RowBatch r) {
    return interOrder.compare(l, l.sorted[l.rowIdx], r, r.sorted[r.rowIdx]);
  }

  public void reset(int capacity) {
    if (queue == null || queue.length < capacity) {
      queue = new RowBatch[capacity];
    }
    clear();
  }

  public void clear() {
    if (queue != null) {
      for (int i = 0; i < queue.length; i ++) {
        queue[i] = null;
      }
    }
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public void add(RowBatch rb) {
    int i = size;
    size = i + 1;
    if (i == 0) {
      queue[0] = rb;
    } else {
      siftUp(i, rb);
    }
  }

  public RowBatch remove() {
    if (size == 0) {
      return null;
    }
    int s = --size;
    RowBatch result = queue[0];
    RowBatch x = queue[s];
    queue[s] = null;
    if (s != 0) {
      siftDown(0, x);
    }
    return result;
  }

  private void siftUp(int k, RowBatch x) {
    while (k > 0) {
      int parent = (k - 1) >>> 1;
      RowBatch e = queue[parent];
      if (compare(x, e) >= 0)
        break;
      queue[k] = e;
      k = parent;
    }
    queue[k] = x;
  }

  private void siftDown(int k, RowBatch x) {
    int half = size >>> 1;
    while (k < half) {
      int child = (k << 1) + 1;
      RowBatch c = queue[child];
      int right = child + 1;
      if (right < size &&
          compare(c, queue[right]) > 0)
        c = queue[child = right];
      if (compare(x, c) <= 0)
        break;
      queue[k] = c;
      k = child;
    }
    queue[k] = x;
  }
}
