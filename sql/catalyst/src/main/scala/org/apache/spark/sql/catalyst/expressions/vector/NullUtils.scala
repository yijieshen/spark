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
        resultCV.isNull(0) = rightCV.isNull(0)
      } else {
        if (selectedInUse) {
          var j = 0
          while (j < n) {
            val i = selected(j)
            resultCV.isNull(i) = rightCV.isNull(i)
            j += 1
          }
        } else {
          System.arraycopy(rightCV.isNull, 0, resultCV.isNull, 0, n)
        }
      }
    } else if (!leftCV.noNulls && rightCV.noNulls) {
      if (leftCV.isRepeating) {
        resultCV.isNull(0) = leftCV.isNull(0)
      } else {
        if (selectedInUse) {
          var j = 0
          while (j < n) {
            val i = selected(j)
            resultCV.isNull(i) = leftCV.isNull(i)
            j += 1
          }
        } else {
          System.arraycopy(leftCV.isNull, 0, resultCV.isNull, 0, n)
        }
      }
    } else if (!leftCV.noNulls && !rightCV.noNulls) {
      if (leftCV.isRepeating && rightCV.isRepeating) {
        resultCV.isNull(0) = leftCV.isNull(0) || rightCV.isNull(0)
        if (resultCV.isNull(0)) {
          resultCV.isRepeating = true
          return
        }
      } else if (leftCV.isRepeating && !rightCV.isRepeating) {
        if (leftCV.isNull(0)) {
          resultCV.isNull(0) = true
          resultCV.isRepeating = true
          return
        } else {
          if (selectedInUse) {
            var j = 0
            while (j < n) {
              val i = selected(j)
              resultCV.isNull(i) = rightCV.isNull(i)
              j += 1
            }
          } else {
            System.arraycopy(rightCV.isNull, 0, resultCV.isNull, 0, n)
          }
        }
      } else if (!leftCV.isRepeating && rightCV.isRepeating) {
        if (rightCV.isNull(0)) {
          resultCV.isNull(0) = true
          resultCV.isRepeating = true
          return
        } else {
          if (selectedInUse) {
            var j = 0
            while (j < n) {
              val i = selected(j)
              resultCV.isNull(i) = leftCV.isNull(i)
              j += 1
            }
          } else {
            System.arraycopy(leftCV.isNull, 0, resultCV.isNull, 0, n)
          }
        }
      } else { // neither side is repeating
        if (selectedInUse) {
          var j = 0
          while (j < n) {
            val i = selected(j)
            resultCV.isNull(i) = leftCV.isNull(i) || rightCV.isNull(i)
            j += 1
          }
        } else {
          var i = 0
          while (i < n) {
            resultCV.isNull(i) = leftCV.isNull(i) || rightCV.isNull(i)
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
      if (rightCV.intVector(0) == 0) {
        hasDivideByZero = true
        resultCV.noNulls = false
        resultCV.isRepeating = true
        resultCV.isNull(0) = true
      }
    } else {
      if (selectedInUse) {
        var j = 0
        while (j < n) {
          val i = selected(j)
          if (rightCV.intVector(i) == 0) {
            resultCV.isNull(i) = true
            rightCV.intVector(i) = 1 // TODO: check spark sql's divide by zero result
            hasDivideByZero = true
          }
          j += 1
        }
      } else {
        var i = 0
        while (i < n) {
          if (rightCV.intVector(i) == 0) {
            resultCV.isNull(i) = true
            rightCV.intVector(i) = 1
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
      if (rightCV.longVector(0) == 0) {
        hasDivideByZero = true
        resultCV.noNulls = false
        resultCV.isRepeating = true
        resultCV.isNull(0) = true
      }
    } else {
      if (selectedInUse) {
        var j = 0
        while (j < n) {
          val i = selected(j)
          if (rightCV.longVector(i) == 0) {
            resultCV.isNull(i) = true
            rightCV.longVector(i) = 1L // TODO: check spark sql's divide by zero result
            hasDivideByZero = true
          }
          j += 1
        }
      } else {
        var i = 0
        while (i < n) {
          if (rightCV.longVector(i) == 0) {
            resultCV.isNull(i) = true
            rightCV.longVector(i) = 1L
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
      if (rightCV.doubleVector(0) == 0.0) {
        hasDivideByZero = true
        resultCV.noNulls = false
        resultCV.isRepeating = true
        resultCV.isNull(0) = true
      }
    } else {
      if (selectedInUse) {
        var j = 0
        while (j < n) {
          val i = selected(j)
          if (rightCV.doubleVector(i) == 0.0) {
            resultCV.isNull(i) = true
            rightCV.doubleVector(i) = 1.0 // TODO: check spark sql's divide by zero result
            hasDivideByZero = true
          }
          j += 1
        }
      } else {
        var i = 0
        while (i < n) {
          if (rightCV.doubleVector(i) == 0.0) {
            resultCV.isNull(i) = true
            rightCV.doubleVector(i) = 1.0
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
    if (v.noNulls) {
      return
    } else if (v.isRepeating && v.isNull(0)) {
      v.intVector(0) = 1
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = sel(j)
        if (v.isNull(i)) {
          v.intVector(i) = 1
        }
        j += 1
      }
    } else {
      var i = 0
      while (i < n) {
        if (v.isNull(i)) {
          v.intVector(i) = 1
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
    if (v.noNulls) {
      return
    } else if (v.isRepeating && v.isNull(0)) {
      v.longVector(0) = 1L
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = sel(j)
        if (v.isNull(i)) {
          v.longVector(i) = 1L
        }
        j += 1
      }
    } else {
      var i = 0
      while (i < n) {
        if (v.isNull(i)) {
          v.longVector(i) = 1L
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
    if (v.noNulls) {
      return
    } else if (v.isRepeating && v.isNull(0)) {
      v.doubleVector(0) = Double.NaN
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = sel(j)
        if (v.isNull(i)) {
          v.doubleVector(i) = Double.NaN
        }
        j += 1
      }
    } else {
      var i = 0
      while (i < n) {
        if (v.isNull(i)) {
          v.doubleVector(i) = Double.NaN
        }
        i += 1
      }
    }
  }

  def filterNulls(cv: ColumnVector, selectedInUse: Boolean, selected: Array[Int], n: Int): Int = {
    var newSize = 0
    if (cv.noNulls) {
      n
    } else if (cv.isRepeating) {
      if (cv.isNull(0)) 0 else n
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = selected(j)
        if (!cv.isNull(i)) {
          selected(newSize) = i
          newSize += 1
        }
        j += 1
      }
      newSize
    } else {
      var i = 0
      while (i < n) {
        if (!cv.isNull(i)) {
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
    var newSize = 0
    if (cv.noNulls) {
      0
    } else if (cv.isRepeating) {
      if (cv.isNull(0)) n else 0
    } else if (selectedInUse) {
      var j = 0
      while (j < n) {
        val i = selected(j)
        if (cv.isNull(i)) {
          selected(newSize) = i
          newSize += 1
        }
        j += 1
      }
      newSize
    } else {
      var i = 0
      while (i < n) {
        if (cv.isNull(i)) {
          selected(newSize) = i
          newSize += 1
        }
        i += 1
      }
      newSize
    }
  }
}
