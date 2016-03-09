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

package org.apache.spark.sql.catalyst.expressions.vector;

import scala.math.Ordering;

import java.util.Arrays;
import java.util.Comparator;

public class SortUtils {

  /**
   * Sort `indies` array by `sortedBy` array, sorted indies are saved in-place.
   * Note: indies and selected should all have size n
   *
   * @param sortedBy sort row idx by this value, for example, the partition ids of this RowBatch
   * @param indices
   * @param selected
   * @param n
   * @param selectedInUse
   */
  public static void sortIndexBy(final int[] sortedBy, Integer[] indices, int[] selected, int n, boolean selectedInUse) {
    if (selectedInUse) {
      for (int i = 0; i < n; i ++) {
        indices[i] = selected[i];
      }
    } else {
      for (int i = 0; i < n; i ++) {
        indices[i] = i;
      }
    }
    Arrays.sort(indices, 0, n, new Ordering<Integer>() {
      public int compare(Integer i1, Integer i2) {
        return Integer.compare(sortedBy[i1], sortedBy[i2]);
      }
    });
  }


  /**
   * Sort `indies` array by `sortedBy` array, sorted indies are saved in-place.
   * Note: indies and selected should all have size n
   *
   * @param indices
   * @param selected
   * @param n
   * @param selectedInUse
   */
  public static void sortIndexBy(Comparator<Integer> comparator, Integer[] indices, int[] selected, int n, boolean selectedInUse) {
    if (selectedInUse) {
      for (int i = 0; i < n; i ++) {
        indices[i] = selected[i];
      }
    } else {
      for (int i = 0; i < n; i ++) {
        indices[i] = i;
      }
    }
    Arrays.sort(indices, 0, n, comparator);
  }
}
