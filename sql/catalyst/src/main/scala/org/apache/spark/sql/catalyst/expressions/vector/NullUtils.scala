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

    resultCV.noNulls = leftCV.noNulls && rightCV.noNulls
    if (leftCV.noNulls && !rightCV.noNulls) {
      if (rightCV.isRepeating) {
        resultCV.setNull(0, rightCV.isNullAt(0))
      } else {
        if (selectedInUse) {
          var j = 0
          while (j < n) {
            val i = selected(j)
            resultCV.setNull(i, rightCV.isNullAt(i))
            j += 1
          }
        } else {
          var i = 0
          while (i < n) {
            resultCV.setNull(i, rightCV.isNullAt(i))
            i += 1
          }
        }
      }
    } else if (!leftCV.noNulls && rightCV.noNulls) {
      if (leftCV.isRepeating) {
        resultCV.setNull(0, leftCV.isNullAt(0))
      } else {
        if (selectedInUse) {
          var j = 0
          while (j < n) {
            val i = selected(j)
            resultCV.setNull(i, leftCV.isNullAt(i))
            j += 1
          }
        } else {
          var i = 0
          while (i < n) {
            resultCV.setNull(i, leftCV.isNullAt(i))
            i += 1
          }
        }
      }
    } else if (!leftCV.noNulls && !rightCV.noNulls) {
      if (leftCV.isRepeating && rightCV.isRepeating) {
        resultCV.setNull(0, leftCV.isNullAt(0) || rightCV.isNullAt(0))
        if (resultCV.isNullAt(0)) {
          resultCV.isRepeating = true
          return
        }
      } else if (leftCV.isRepeating && !rightCV.isRepeating) {
        if (leftCV.isNullAt(0)) {
          resultCV.setNull(0, true)
          resultCV.isRepeating = true
          return
        } else {
          if (selectedInUse) {
            var j = 0
            while (j < n) {
              val i = selected(j)
              resultCV.setNull(i, rightCV.isNullAt(i))
              j += 1
            }
          } else {
            var i = 0
            while (i < n) {
              resultCV.setNull(i, rightCV.isNullAt(i))
              i += 1
            }
          }
        }
      } else if (!leftCV.isRepeating && rightCV.isRepeating) {
        if (rightCV.isNullAt(0)) {
          resultCV.setNull(0, true)
          resultCV.isRepeating = true
          return
        } else {
          if (selectedInUse) {
            var j = 0
            while (j < n) {
              val i = selected(j)
              resultCV.setNull(i, leftCV.isNullAt(i))
              j += 1
            }
          } else {
            var i = 0
            while (i < n) {
              resultCV.setNull(i, leftCV.isNullAt(i))
              i += 1
            }
          }
        }
      } else { // neither side is repeating
        if (selectedInUse) {
          var j = 0
          while (j < n) {
            val i = selected(j)
            resultCV.setNull(i, leftCV.isNullAt(i) || rightCV.isNullAt(i))
            j += 1
          }
        } else {
          var i = 0
          while (i < n) {
            resultCV.setNull(i, leftCV.isNullAt(i) || rightCV.isNullAt(i))
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

    var hasDivideByZero = false
    if (rightCV.isRepeating) {
      if (rightCV.getInt(0) == 0) {
        hasDivideByZero = true
        resultCV.noNulls = false
        resultCV.isRepeating = true
        resultCV.setNull(0, true)
      }
    } else {
      if (selectedInUse) {
        var j = 0
        while (j < n) {
          val i = selected(j)
          if (rightCV.getInt(i) == 0) {
            resultCV.setNull(i, true)
            rightCV.putInt(i, 1) // TODO: check spark sql's divide by zero result
            hasDivideByZero = true
          }
          j += 1
        }
      } else {
        var i = 0
        while (i < n) {
          if (rightCV.getInt(i) == 0) {
            resultCV.setNull(i, true)
            rightCV.putInt(i, 1)
            hasDivideByZero = true
          }
          i += 1
        }
      }
    }

    resultCV.noNulls = resultCV.noNulls && !hasDivideByZero
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

    var hasDivideByZero = false
    if (rightCV.isRepeating) {
      if (rightCV.getLong(0) == 0) {
        hasDivideByZero = true
        resultCV.noNulls = false
        resultCV.isRepeating = true
        resultCV.setNull(0, true)
      }
    } else {
      if (selectedInUse) {
        var j = 0
        while (j < n) {
          val i = selected(j)
          if (rightCV.getLong(i) == 0) {
            resultCV.setNull(i, true)
            rightCV.putLong(i, 1) // TODO: check spark sql's divide by zero result
            hasDivideByZero = true
          }
          j += 1
        }
      } else {
        var i = 0
        while (i < n) {
          if (rightCV.getLong(i) == 0) {
            resultCV.setNull(i, true)
            rightCV.putLong(i, 1)
            hasDivideByZero = true
          }
          i += 1
        }
      }
    }

    resultCV.noNulls = resultCV.noNulls && !hasDivideByZero
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

    var hasDivideByZero = false
    if (rightCV.isRepeating) {
      if (rightCV.getDouble(0) == 0) {
        hasDivideByZero = true
        resultCV.noNulls = false
        resultCV.isRepeating = true
        resultCV.setNull(0, true)
      }
    } else {
      if (selectedInUse) {
        var j = 0
        while (j < n) {
          val i = selected(j)
          if (rightCV.getDouble(i) == 0) {
            resultCV.setNull(i, true)
            rightCV.putDouble(i, 1.0) // TODO: check spark sql's divide by zero result
            hasDivideByZero = true
          }
          j += 1
        }
      } else {
        var i = 0
        while (i < n) {
          if (rightCV.getDouble(i) == 0) {
            resultCV.setNull(i, true)
            rightCV.putDouble(i, 1.0)
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
  def setNullDataEntriesInteger(cv: ColumnVector,
      selectedInUse: Boolean, sel: Array[Int], n: Int): Unit = {

    if (cv.noNulls) {
      return
    } else if (cv.isRepeating && cv.isNullAt(0)) {
      cv.putInt(0, 1)
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = sel(j)
        if (cv.isNullAt(i)) {
          cv.putInt(i, 1)
        }
        j += 1
      }
    } else {
      var i = 0
      while (i < n) {
        if (cv.isNullAt(i)) {
          cv.putInt(i, 1)
        }
        i += 1
      }
    }
  }

  /**
    * Set the data value for all NULL entries to the designated NULL_VALUE.
    */
  def setNullDataEntriesLong(cv: ColumnVector,
    selectedInUse: Boolean, sel: Array[Int], n: Int): Unit = {

    if (cv.noNulls) {
      return
    } else if (cv.isRepeating && cv.isNullAt(0)) {
      cv.putLong(0, 1L)
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = sel(j)
        if (cv.isNullAt(i)) {
          cv.putLong(i, 1L)
        }
        j += 1
      }
    } else {
      var i = 0
      while (i < n) {
        if (cv.isNullAt(i)) {
          cv.putLong(i, 1L)
        }
        i += 1
      }
    }
  }

  /**
    * Set the data value for all NULL entries to the designated NULL_VALUE.
    */
  def setNullDataEntriesDouble(cv: ColumnVector,
    selectedInUse: Boolean, sel: Array[Int], n: Int): Unit = {

    if (cv.noNulls) {
      return
    } else if (cv.isRepeating && cv.isNullAt(0)) {
      cv.putDouble(0, Double.NaN)
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = sel(j)
        if (cv.isNullAt(i)) {
          cv.putDouble(i, Double.NaN)
        }
        j += 1
      }
    } else {
      var i = 0
      while (i < n) {
        if (cv.isNullAt(i)) {
          cv.putDouble(i, Double.NaN)
        }
        i += 1
      }
    }
  }

  def filterNulls(v: ColumnVector, selectedInUse: Boolean, selected: Array[Int], n: Int): Int = {

    var newSize = 0
    if (v.noNulls) {
      n
    } else if (v.isRepeating) {
      if (v.isNullAt(0)) 0 else n
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = selected(j)
        if (!v.isNullAt(i)) {
          selected(newSize) = i
          newSize += 1
        }
        j += 1
      }
      newSize
    } else {
      var i = 0
      while (i < n) {
        if (!v.isNullAt(i)) {
          selected(newSize) = i
          newSize += 1
        }
        i += 1
      }
      newSize
    }
  }

  def filterNonNulls(
      v: ColumnVector, selectedInUse: Boolean, selected: Array[Int], n: Int): Int = {

    var newSize = 0
    if (v.noNulls) {
      0
    } else if (v.isRepeating) {
      if (v.isNullAt(0)) n else 0
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = selected(j)
        if (v.isNullAt(i)) {
          selected(newSize) = i
          newSize += 1
        }
        j += 1
      }
      newSize
    } else {
      var i = 0
      while (i < n) {
        if (v.isNullAt(i)) {
          selected(newSize) = i
          newSize += 1
        }
        i += 1
      }
      newSize
    }
  }
}
