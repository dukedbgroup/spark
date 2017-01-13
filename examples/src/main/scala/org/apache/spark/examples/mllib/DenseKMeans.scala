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
package org.apache.spark.examples.mllib

import java.io._
import java.lang.System
import java.text._
import java.util.Date
import java.util.concurrent._

import scala.util.Properties

import org.apache.hadoop.io.LongWritable
import org.apache.log4j.{Level, Logger}
import org.apache.mahout.math.VectorWritable
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * An example k-means app. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.DenseKMeans [options] <input>
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object DenseKMeans {

  object InitializationMode extends Enumeration {
    type InitializationMode = Value
    val Random, Parallel = Value
  }

  import InitializationMode._

  case class Params(
      input: String = null,
      k: Int = -1,
      numIterations: Int = 10,
      // han
      storageLevel: String = "NONE",
      initializationMode: InitializationMode = Parallel) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DenseKMeans") {
      head("DenseKMeans: an example k-means app for dense data.")
      opt[Int]('k', "k")
        .required()
        .text(s"number of clusters, required")
        .action((x, c) => c.copy(k = x))
      // han
      opt[String]("storageLevel")
        .required()
        .text(s"One of (NONE, MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY) required")
        .action((x, c) => c.copy(storageLevel = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[String]("initMode")
        .text(s"initialization mode (${InitializationMode.values.mkString(",")}), " +
        s"default: ${defaultParams.initializationMode}")
        .action((x, c) => c.copy(initializationMode = InitializationMode.withName(x)))
      arg[String]("<input>")
        .text("input paths to examples")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"DenseKMeans with $params")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    var storageLevel: StorageLevel = null
    storageLevel = params.storageLevel match {
      case "NONE" => StorageLevel.NONE
      case "MEMORY_ONLY" => StorageLevel.MEMORY_ONLY
      case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
      case "DISK_ONLY" => StorageLevel.DISK_ONLY
      case "MEMORY_ONLY_SER" => StorageLevel.MEMORY_ONLY_SER
    }

    var examples: RDD[Vector] = null
/*
    val examples = sc.textFile(params.input).map { line =>
      Vectors.dense(line.split(' ').map(_.toDouble))
    }.cache()
*/

    val data = sc.sequenceFile[LongWritable, VectorWritable](params.input)
    examples = data.map {
      case (k, v) =>
        var vector: Array[Double] = new Array[Double](v.get().size)
        for (i <- 0 until v.get().size) vector(i) = v.get().get(i)
        Vectors.dense(vector)
    }.persist(storageLevel)

    val numExamples = examples.count()

    println(s"numExamples = $numExamples.")

    val initMode = params.initializationMode match {
      case Random => KMeans.RANDOM
      case Parallel => KMeans.K_MEANS_PARALLEL
    }

    val model = new KMeans()
      .setInitializationMode(initMode)
      .setK(params.k)
      .setMaxIterations(params.numIterations)
      .setStorageLevel(storageLevel)
      .run(examples)

    val cost = model.computeCost(examples)

    println(s"Total cost = $cost.")

    sc.stop()
  }
}
// scalastyle:on println
