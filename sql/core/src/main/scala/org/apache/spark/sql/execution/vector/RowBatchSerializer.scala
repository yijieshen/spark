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

package org.apache.spark.sql.execution.vector

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.catalyst.vector.RowBatch
import org.apache.spark.sql.types.DataType

class RowBatchSerializer(schema: Array[DataType]) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new RowBatchSerializerInstance(schema)
  override private[spark] def supportsRelocationOfSerializedObjects: Boolean = true
}

private class RowBatchSerializerInstance(schema: Array[DataType]) extends SerializerInstance {
  /**
    * Serializes a stream of UnsafeRows. Within the stream, each record consists of a record
    * length (stored as a 4-byte integer, written high byte first), followed by the record's bytes.
    */
  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] val dOut: DataOutputStream =
      new DataOutputStream(new BufferedOutputStream(out))

    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      val rb = value.asInstanceOf[RowBatch]

      dOut.writeInt(rb.size)
      rb.writeToStream(dOut);
      this
    }

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(null == key || key.isInstanceOf[Int])
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def flush(): Unit = {
      dOut.flush()
    }

    override def close(): Unit = {
      dOut.close()
    }
  }

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private[this] val dIn: DataInputStream = new DataInputStream(new BufferedInputStream(in))
      private[this] var rowBatch: RowBatch = RowBatch.create(schema, RowBatch.DEFAULT_SIZE)
      private[this] var batchTuple: (Int, RowBatch) = (0, rowBatch)

      private[this] val EOF: Int = -1

      override def asKeyValueIterator: Iterator[(Int, RowBatch)] = {
        new Iterator[(Int, RowBatch)] {

          private[this] def readSize(): Int = try {
            dIn.readInt()
          } catch {
            case e: EOFException =>
              dIn.close()
              EOF
          }

          private[this] var batchSize: Int = readSize()
          override def hasNext: Boolean = batchSize != EOF

          override def next(): (Int, RowBatch) = {
            if (rowBatch.capacity < batchSize) {
              rowBatch = RowBatch.create(schema, batchSize)
            }
            rowBatch.size = batchSize
            var i = 0
            while (i < schema.length) {
              rowBatch.columns(i).readFromStream(dIn)
              i += 1
            }
            batchSize = readSize()
            if (batchSize == EOF) { // We are returning the last row in this stream
              dIn.close()
              val _batchTuple = batchTuple
              // Null these out so that the byte array can be garbage collected once the entire
              // iterator has been consumed
              rowBatch = null
              batchTuple = null
              _batchTuple
            } else {
              batchTuple
            }
          }
        }
      }

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      override def readValue[T: ClassTag](): T = {
        val batchSize = dIn.readInt()
        if (rowBatch.capacity < batchSize) {
          rowBatch = RowBatch.create(schema, batchSize)
        }
        rowBatch.size = batchSize
        var i = 0
        while (i < schema.length) {
          rowBatch.columns(i).readFromStream(dIn)
          i += 1
        }
        rowBatch.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        dIn.close()
      }
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}
