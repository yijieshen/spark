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

package org.apache.spark.sql.catalyst.expressions.vector

import org.apache.spark.sql.catalyst.vector._

object NullUtils {

  /**
   * propagate null values for binary expression
   */
  def propagateNullsForBinaryExpression(
      leftCV: ColumnVector,
      rightCV: ColumnVector,
      resultCV: ColumnVector,
      selected: Array[Int],
      n: Int,
      selectedInUse: Boolean): Unit = {

    val left = leftCV.asInstanceOf[OnColumnVector]
    val right = rightCV.asInstanceOf[OnColumnVector]
    val result = resultCV.asInstanceOf[OnColumnVector]

    result.noNulls = leftCV.noNulls && right.noNulls
    if (left.noNulls && !right.noNulls) {
      if (right.isRepeating) {
        result.isNull(0) = right.isNull(0)
      } else {
        if (selectedInUse) {
          var j = 0
          while (j < n) {
            val i = selected(j)
            result.isNull(i) = right.isNull(i)
            j += 1
          }
        } else {
          System.arraycopy(right.isNull, 0, result.isNull, 0, n)
        }
      }
    } else if (!left.noNulls && right.noNulls) {
      if (left.isRepeating) {
        result.isNull(0) = left.isNull(0)
      } else {
        if (selectedInUse) {
          var j = 0
          while (j < n) {
            val i = selected(j)
            result.isNull(i) = left.isNull(i)
            j += 1
          }
        } else {
          System.arraycopy(left.isNull, 0, result.isNull, 0, n)
        }
      }
    } else if (!left.noNulls && !right.noNulls) {
      if (left.isRepeating && right.isRepeating) {
        result.isNull(0) = left.isNull(0) || right.isNull(0)
        if (result.isNull(0)) {
          result.isRepeating = true
          return
        }
      } else if (left.isRepeating && !right.isRepeating) {
        if (left.isNull(0)) {
          result.isNull(0) = true
          result.isRepeating = true
          return
        } else {
          if (selectedInUse) {
            var j = 0
            while (j < n) {
              val i = selected(j)
              result.isNull(i) = right.isNull(i)
              j += 1
            }
          } else {
            System.arraycopy(right.isNull, 0, result.isNull, 0, n)
          }
        }
      } else if (!left.isRepeating && right.isRepeating) {
        if (right.isNull(0)) {
          result.isNull(0) = true
          result.isRepeating = true
          return
        } else {
          if (selectedInUse) {
            var j = 0
            while (j < n) {
              val i = selected(j)
              result.isNull(i) = left.isNull(i)
              j += 1
            }
          } else {
            System.arraycopy(left.isNull, 0, result.isNull, 0, n)
          }
        }
      } else { // neither side is repeating
        if (selectedInUse) {
          var j = 0
          while (j < n) {
            val i = selected(j)
            result.isNull(i) = left.isNull(i) || right.isNull(i)
            j += 1
          }
        } else {
          var i = 0
          while (i < n) {
            result.isNull(i) = left.isNull(i) || right.isNull(i)
            i += 1
          }
        }
      }
    }
  }

  /**
   * 5 / 0 = null and change 0 to 1 to avoid special care while processing in batch
   */
  def propagateZeroDenomAsNullsInteger(
      rightCV: ColumnVector,
      resultCV: ColumnVector,
      selected: Array[Int],
      n: Int,
      selectedInUse: Boolean): Unit = {

    val right = rightCV.asInstanceOf[OnColumnVector]
    val result = resultCV.asInstanceOf[OnColumnVector]

    var hasDivideByZero = false
    if (right.isRepeating) {
      if (right.intVector(0) == 0) {
        hasDivideByZero = true
        result.noNulls = false
        result.isRepeating = true
        result.isNull(0) = true
      }
    } else {
      if (selectedInUse) {
        var j = 0
        while (j < n) {
          val i = selected(j)
          if (right.intVector(i) == 0) {
            result.isNull(i) = true
            right.intVector(i) = 1 // TODO: check spark sql's divide by zero result
            hasDivideByZero = true
          }
          j += 1
        }
      } else {
        var i = 0
        while (i < n) {
          if (right.intVector(i) == 0) {
            result.isNull(i) = true
            right.intVector(i) = 1
            hasDivideByZero = true
          }
          i += 1
        }
      }
    }

    result.noNulls = result.noNulls && !hasDivideByZero
  }

  /**
    * 5 / 0 = null and change 0 to 1 to avoid special care while processing in batch
    */
  def propagateZeroDenomAsNullsLong(
    rightCV: ColumnVector,
    resultCV: ColumnVector,
    selected: Array[Int],
    n: Int,
    selectedInUse: Boolean): Unit = {

    val right = rightCV.asInstanceOf[OnColumnVector]
    val result = resultCV.asInstanceOf[OnColumnVector]

    var hasDivideByZero = false
    if (right.isRepeating) {
      if (right.longVector(0) == 0) {
        hasDivideByZero = true
        result.noNulls = false
        result.isRepeating = true
        result.isNull(0) = true
      }
    } else {
      if (selectedInUse) {
        var j = 0
        while (j < n) {
          val i = selected(j)
          if (right.longVector(i) == 0) {
            result.isNull(i) = true
            right.longVector(i) = 1L // TODO: check spark sql's divide by zero result
            hasDivideByZero = true
          }
          j += 1
        }
      } else {
        var i = 0
        while (i < n) {
          if (right.longVector(i) == 0) {
            result.isNull(i) = true
            right.longVector(i) = 1L
            hasDivideByZero = true
          }
          i += 1
        }
      }
    }

    result.noNulls = result.noNulls && !hasDivideByZero
  }

  /**
    * 5 / 0 = null and change 0 to 1 to avoid special care while processing in batch
    */
  def propagateZeroDenomAsNullsDouble(
    rightCV: ColumnVector,
    resultCV: ColumnVector,
    selected: Array[Int],
    n: Int,
    selectedInUse: Boolean): Unit = {

    val right = rightCV.asInstanceOf[OnColumnVector]
    val result = resultCV.asInstanceOf[OnColumnVector]

    var hasDivideByZero = false
    if (right.isRepeating) {
      if (right.doubleVector(0) == 0.0) {
        hasDivideByZero = true
        result.noNulls = false
        result.isRepeating = true
        result.isNull(0) = true
      }
    } else {
      if (selectedInUse) {
        var j = 0
        while (j < n) {
          val i = selected(j)
          if (right.doubleVector(i) == 0.0) {
            result.isNull(i) = true
            right.doubleVector(i) = 1.0 // TODO: check spark sql's divide by zero result
            hasDivideByZero = true
          }
          j += 1
        }
      } else {
        var i = 0
        while (i < n) {
          if (right.doubleVector(i) == 0.0) {
            result.isNull(i) = true
            right.doubleVector(i) = 1.0
            hasDivideByZero = true
          }
          i += 1
        }
      }
    }

    resultCV.noNulls = resultCV.noNulls && !hasDivideByZero
  }

  /**
    * Set the data value for all NULL entries to the designated NULL_VALUE.
    */
  def setNullDataEntriesInteger(v: ColumnVector,
      selectedInUse: Boolean, sel: Array[Int], n: Int): Unit = {

    val cv = v.asInstanceOf[OnColumnVector]

    if (cv.noNulls) {
      return
    } else if (cv.isRepeating && cv.isNull(0)) {
      cv.intVector(0) = 1
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = sel(j)
        if (cv.isNull(i)) {
          cv.intVector(i) = 1
        }
        j += 1
      }
    } else {
      var i = 0
      while (i < n) {
        if (cv.isNull(i)) {
          cv.intVector(i) = 1
        }
        i += 1
      }
    }
  }

  /**
    * Set the data value for all NULL entries to the designated NULL_VALUE.
    */
  def setNullDataEntriesLong(v: ColumnVector,
    selectedInUse: Boolean, sel: Array[Int], n: Int): Unit = {

    val cv = v.asInstanceOf[OnColumnVector]

    if (cv.noNulls) {
      return
    } else if (cv.isRepeating && cv.isNull(0)) {
      cv.longVector(0) = 1L
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = sel(j)
        if (cv.isNull(i)) {
          cv.longVector(i) = 1L
        }
        j += 1
      }
    } else {
      var i = 0
      while (i < n) {
        if (cv.isNull(i)) {
          cv.longVector(i) = 1L
        }
        i += 1
      }
    }
  }

  /**
    * Set the data value for all NULL entries to the designated NULL_VALUE.
    */
  def setNullDataEntriesDouble(v: ColumnVector,
    selectedInUse: Boolean, sel: Array[Int], n: Int): Unit = {

    val cv = v.asInstanceOf[OnColumnVector]

    if (cv.noNulls) {
      return
    } else if (cv.isRepeating && cv.isNull(0)) {
      cv.doubleVector(0) = Double.NaN
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = sel(j)
        if (cv.isNull(i)) {
          cv.doubleVector(i) = Double.NaN
        }
        j += 1
      }
    } else {
      var i = 0
      while (i < n) {
        if (cv.isNull(i)) {
          cv.doubleVector(i) = Double.NaN
        }
        i += 1
      }
    }
  }

  def filterNulls(cv: ColumnVector, selectedInUse: Boolean, selected: Array[Int], n: Int): Int = {

    val v = cv.asInstanceOf[OnColumnVector]

    var newSize = 0
    if (v.noNulls) {
      n
    } else if (v.isRepeating) {
      if (v.isNull(0)) 0 else n
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = selected(j)
        if (!v.isNull(i)) {
          selected(newSize) = i
          newSize += 1
        }
        j += 1
      }
      newSize
    } else {
      var i = 0
      while (i < n) {
        if (!v.isNull(i)) {
          selected(newSize) = i
          newSize += 1
        }
        i += 1
      }
      newSize
    }
  }

  def filterNonNulls(
      cv: ColumnVector, selectedInUse: Boolean, selected: Array[Int], n: Int): Int = {
    val v = cv.asInstanceOf[OnColumnVector]

    var newSize = 0
    if (v.noNulls) {
      0
    } else if (v.isRepeating) {
      if (v.isNull(0)) n else 0
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = selected(j)
        if (v.isNull(i)) {
          selected(newSize) = i
          newSize += 1
        }
        j += 1
      }
      newSize
    } else {
      var i = 0
      while (i < n) {
        if (v.isNull(i)) {
          selected(newSize) = i
          newSize += 1
        }
        i += 1
      }
      newSize
    }
  }
}
