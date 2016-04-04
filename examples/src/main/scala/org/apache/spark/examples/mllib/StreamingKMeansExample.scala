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

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.examples.streaming.StreamingExamples;

// han
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.examples.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream._
import org.apache.spark.storage.StorageLevel

/**
 * Estimate clusters on one stream of data and make predictions
 * on another stream, where the data streams arrive as text files
 * into two different directories.
 *
 * The rows of the training text files must be vector data in the form
 * `[x1,x2,x3,...,xn]`
 * Where n is the number of dimensions.
 *
 * The rows of the test text files must be labeled data in the form
 * `(y,[x1,x2,x3,...,xn])`
 * Where y is some identifier. n must be the same for train and test.
 *
 * Usage:
 *   StreamingKMeansExample <trainingDir> <testDir> <batchDuration> <numClusters> <numDimensions>
 *
 * To run on your local machine using the two directories `trainingDir` and `testDir`,
 * with updates every 5 seconds, 2 dimensions per data point, and 3 clusters, call:
 *    $ bin/run-example mllib.StreamingKMeansExample trainingDir testDir 5 3 2
 *
 * As you add text files to `trainingDir` the clusters will continuously update.
 * Anytime you add text files to `testDir`, you'll see predicted labels using the current model.
 *
 */
object StreamingKMeansExample {


//hibench.kmeans.huge.num_of_clusters             5
//hibench.kmeans.huge.dimensions                  20
//hibench.kmeans.huge.num_of_samples              100000000
//hibench.kmeans.huge.samples_per_inputfile       20000000
//hibench.kmeans.huge.max_iteration               5
//hibench.kmeans.huge.k                           10
//hibench.kmeans.huge.convergedist                0.5


  def main(args: Array[String]) {
    if (args.length != 5) {

    System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics> <batchDuration> <numClusters> <numDimensions>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)


      //System.err.println(
      //  "Usage: StreamingKMeansExample " +
      //    "> <testDir> <batchDuration> <numClusters> <numDimensions>")
      //System.exit(1)
    }

	// han
    //val conf = new SparkConf().setMaster("local").setAppName("StreamingKMeansExample")
    //val ssc = new StreamingContext(conf, Seconds(args(2).toLong))


	// han
    StreamingExamples.setStreamingLogLevels()
    val Array(brokers, topics)  = args.slice(0, 2)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamingKMeansExample")
    val ssc = new StreamingContext(sparkConf, Seconds(args(2).toLong))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

	var trainingData = createDirectStream(ssc, kafkaParams, topicsSet).map(_._2).map(Vectors.parse)

	// han
    //val trainingData = ssc.textFileStream(args(0)).map(Vectors.parse)
    //val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)

    val model = new StreamingKMeans()
      .setK(args(3).toInt)
      .setDecayFactor(1.0)
      .setRandomCenters(args(4).toInt, 0.0)

    	model.trainOn(trainingData)
	println(model.latestModel)
	
	// han
    //model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
  }

		def createDirectStream(ssc:StreamingContext, kafkaParams:Map[String, String], topicSet:Set[String]):DStream[(String, String)]={
			println(s"Create direct kafka stream, args:$kafkaParams")
			KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
		}
}
// scalastyle:on println
