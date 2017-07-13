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

// scalastyle:off println
package org.apache.spark.examples

import java.io._
import java.lang.System
import java.text._
import java.util.regex.Pattern

import scala.Tuple2

import org.apache.spark.sql.SparkSession

case class Counts(word: String, count: Int)

object Micro {

  val SPACE = Pattern.compile(" ")

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: Micro <file> <shuffle-ratio> <iteration>");
      System.exit(1);
    }

    val path = args(0)
    val shuffleRatio = args(1).toDouble
    val iterations = args(2).toInt

    val spark = SparkSession.builder()
.appName("Micro : " + args(0) + " : " + args(1) + " : " + args(2))
.getOrCreate()
    import spark.implicits._

    val lines = spark.read.textFile(path)
    val data = lines.map((_, 1))
    // var data = lines.map((_, 1)).map(t => Counts(t._1, t._2)).toDF()
    // data.registerTempTable("data")
    // cache if iterative
    // if(iterations > 1) {
    //   data = data.cache
    // }

    // run iterations
    for (i <- 1 to iterations) {
      // cache data
      if (iterations > 1) { data.persist }

      // convert to df
      val asDF = data.map(t => Counts(t._1, t._2)).toDF()

      // scale when shuffleRatio is higher than 1
      val scaled =
        asDF.flatMap(t => (1 to shuffleRatio.toInt).map { n =>
Counts(t.getAs[String]("word") + n, t.getAs[Int]("count")) }).toDF()

      // sample to account for fractional ratio
      val f: Double = shuffleRatio - shuffleRatio.toInt
      val sampled = asDF.sample(false, f)

      // union datasets
      val toShuffle = sampled union scaled

      // sort data
      val sorted = toShuffle.sort("word").count

      // uncache data
      if (iterations > 1) { data.unpersist }
    }

    // val partitioner = new HashPartitioner(partitions = parallel)
    // val sorted = data.sort("word").select("word")

    // sorted.write.format("text").save(args(1))

    // output.foreach(t => println(t._1 + ": " + t._2))

    spark.stop()

    sys.exit(0)
  }
}
// scalastyle:on println
