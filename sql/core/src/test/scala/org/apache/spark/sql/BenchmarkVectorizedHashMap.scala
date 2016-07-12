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

package org.apache.spark.sql

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.util.Benchmark

class BenchmarkVectorizedHashMap extends SparkFunSuite {
  lazy val conf = new SparkConf().setMaster("local[1]").setAppName("benchmark")
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.sql.autoBroadcastJoinThreshold", "1")
  lazy val sc = SparkContext.getOrCreate(conf)
  lazy val sqlContext = SQLContext.getOrCreate(sc)

  test("aggregate with linear keys") {
    val N = 20 << 20

    val benchmark = new Benchmark("Aggregate w keys", N)
    def f(): Unit = sqlContext.range(N).selectExpr("(id & 50000) as k").groupBy("k").sum().collect()

    benchmark.addCase(s"vectorize = F") { iter =>
      sqlContext.setConf("spark.sql.vectorize.enabled", "false")
      f()
    }

    benchmark.addCase(s"vectorize = T hashmap = F") { iter =>
      sqlContext.setConf("spark.sql.vectorize.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.agg.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.hm.enabled", "false")
      f()
    }

    benchmark.addCase(s"vectorize = T hashmap = T") { iter =>
      sqlContext.setConf("spark.sql.vectorize.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.agg.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.hm.enabled", "true")
      f()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_20-b26 on Mac OS X 10.10.5
    Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz
    Aggregate w keys:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    vectorize = F                            1895 / 1950         11.1          90.3       1.0X
    vectorize = T hashmap = F                1197 / 1391         17.5          57.1       1.6X
    vectorize = T hashmap = T                 373 /  384         56.3          17.8       5.1X
    */
  }

  test("aggregate with randomized keys") {
    val N = 20 << 20

    val benchmark = new Benchmark("Aggregate w keys", N)
    sqlContext.range(N).selectExpr("id", "floor(rand() * 10000) as k").registerTempTable("test")

    def f(): Unit = sqlContext.sql("select k, k, sum(id) from test group by k, k").collect()

    benchmark.addCase(s"vectorize = F") { iter =>
      sqlContext.setConf("spark.sql.vectorize.enabled", "false")
      f()
    }

    benchmark.addCase(s"vectorize = T hashmap = F") { iter =>
      sqlContext.setConf("spark.sql.vectorize.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.agg.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.hm.enabled", "false")
      f()
    }

    benchmark.addCase(s"vectorize = T hashmap = T") { iter =>
      sqlContext.setConf("spark.sql.vectorize.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.agg.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.hm.enabled", "true")
      f()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Aggregate w keys:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    codegen = F                              2517 / 2608          8.3         120.0       1.0X
    codegen = T hashmap = F                  1484 / 1560         14.1          70.8       1.7X
    codegen = T hashmap = T                   794 /  908         26.4          37.9       3.2X
    */
  }

  test("aggregate with string key") {
    val N = 20 << 20

    val benchmark = new Benchmark("Aggregate w string key", N)
    def f(): Unit = sqlContext.range(N).selectExpr("id", "cast(id & 1023 as string) as k")
      .groupBy("k").count().collect()

    benchmark.addCase(s"vectorize = F") { iter =>
      sqlContext.setConf("spark.sql.vectorize.enabled", "false")
      f()
    }

    benchmark.addCase(s"vectorize = T hashmap = F") { iter =>
      sqlContext.setConf("spark.sql.vectorize.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.agg.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.hm.enabled", "false")
      f()
    }

    benchmark.addCase(s"vectorize = T hashmap = T") { iter =>
      sqlContext.setConf("spark.sql.vectorize.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.agg.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.hm.enabled", "true")
      f()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Aggregate w string key:             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    codegen = F                              3307 / 3376          6.3         157.7       1.0X
    codegen = T hashmap = F                  2364 / 2471          8.9         112.7       1.4X
    codegen = T hashmap = T                  1740 / 1841         12.0          83.0       1.9X
    */
  }

  test("aggregate with multiple key types") {
    val N = 20 << 20

    val benchmark = new Benchmark("Aggregate w multiple keys", N)
    def f(): Unit = sqlContext.range(N)
      .selectExpr(
        "id",
        "(id & 1023) as k1",
        "cast(id & 1023 as string) as k2",
        "cast(id & 1023 as int) as k3",
        "cast(id & 1023 as double) as k4",
        "cast(id & 1023 as float) as k5",
        "id > 1023 as k6")
      .groupBy("k1", "k2", "k3", "k4", "k5", "k6")
      .sum()
      .collect()

    benchmark.addCase(s"vectorize = F") { iter =>
      sqlContext.setConf("spark.sql.vectorize.enabled", "false")
      f()
    }

    benchmark.addCase(s"vectorize = T hashmap = F") { iter =>
      sqlContext.setConf("spark.sql.vectorize.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.agg.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.hm.enabled", "false")
      f()
    }

    benchmark.addCase(s"vectorize = T hashmap = T") { iter =>
      sqlContext.setConf("spark.sql.vectorize.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.agg.enabled", "true")
      sqlContext.setConf("spark.sql.vectorize.hm.enabled", "true")
      f()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_73-b02 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Aggregate w decimal key:             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    codegen = F                              5885 / 6091          3.6         280.6       1.0X
    codegen = T hashmap = F                  3625 / 4009          5.8         172.8       1.6X
    codegen = T hashmap = T                  3204 / 3271          6.5         152.8       1.8X
    */
  }

}
