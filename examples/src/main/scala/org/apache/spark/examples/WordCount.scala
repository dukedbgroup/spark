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
import java.util.Arrays
import java.util.Date
import java.util.List
import java.util.concurrent._
import java.util.regex.Pattern

import scala.Tuple2
import scala.util.Properties

import org.apache.spark._
import org.apache.spark.storage._

object WordCount {

  val SPACE = Pattern.compile(" ")

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: WordCount <file>");
      System.exit(1);
    }

    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    // han sampler 1 begin
    val SAMPLING_PERIOD: Long = 10
    val TIMESTAMP_PERIOD: Long = 1000

    var dateFormat: DateFormat = new SimpleDateFormat("hh:mm:ss")

    val dirname_application = Properties.envOrElse("SPARK_HOME",
        "/home/mayuresh/spark-1.5.1") + "/logs/" + sc.applicationId
    val dir_application = new File(dirname_application)
    if (!dir_application.exists()) {
      dir_application.mkdirs()
    }

    val ex = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      var i: Long = 0
      override def run {

        //        sc.getExecutorStorageStatus.filter(s => s.blockManagerId.host.contains("slave1"))
        sc.getExecutorStorageStatus.foreach {
          es =>
            val filename: String = dirname_application +
               "/sparkOutput_driver_"  + sc.applicationId + "_" + es.blockManagerId + ".txt"
            val file = new File(filename)
            val writer = new FileWriter(file, true)
            if (!file.exists()) {
              file.createNewFile()
              writer.write(sc.applicationId + "_" + es.blockManagerId + "\n")
              writer.flush()
              // writer.close()
            }
            var s = es.memUsed.toString()
            // println(s)
            if (i % TIMESTAMP_PERIOD == 0) {
              i = 0
              var time: String = dateFormat.format(new Date())
              s += "\t" + time
            }

            writer.write(s + "\n")
            writer.flush()
            writer.close()
         }
         i = i + SAMPLING_PERIOD
      }
    }
    val f = ex.scheduleAtFixedRate(task, 0, SAMPLING_PERIOD, TimeUnit.MILLISECONDS)
    // han sampler 1 end

    val lines = sc.textFile(args(0), 1)

    val words = lines.flatMap(l => SPACE.split(l))
    val ones = words.map(w => (w, 1))
    val counts = ones.reduceByKey(_ + _)

    // val output = counts.collect()
    // output.foreach(t => println(t._1 + ": " + t._2))
    counts.saveAsTextFile(args(1))

    sc.stop()

    // han sampler 2 begin
    f.cancel(true)
    // hand sampler 2 end
    sys.exit(0)
  }
}
// scalastyle:on println

