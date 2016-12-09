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

package org.apache.spark.executor

import java.io._
import java.lang.System
import java.lang.management._
import java.net.URL
import java.nio.ByteBuffer
import java.text._
import java.util.Date
import java.util.List
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.sys.process._
import scala.util.{Failure, Properties, Success}
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage._
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class CoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    hostname: String,
    cores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv)
  extends ThreadSafeRpcEndpoint with ExecutorBackend with Logging {

  private[this] val stopping = new AtomicBoolean(false)
  var executor: Executor = null
  @volatile var driver: Option[RpcEndpointRef] = None

  // If this CoarseGrainedExecutorBackend is changed to support multiple threads, then this may need
  // to be changed so that we don't share the serializer instance across threads
  private[this] val ser: SerializerInstance = env.closureSerializer.newInstance()

  override def onStart() {
    logInfo("Connecting to driver: " + driverUrl)
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      driver = Some(ref)
      ref.ask[Boolean](RegisterExecutor(executorId, self, hostname, cores, extractLogUrls))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      case Success(msg) =>
        // Always receive `true`. Just ignore it
      case Failure(e) =>
        exitExecutor(1, s"Cannot register with driver: $driverUrl", e)
    }(ThreadUtils.sameThread)
  }

  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase, e._2))
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      try {
        executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)
      } catch {
        case NonFatal(e) =>
          exitExecutor(1, "Unable to create executor due to " + e.getMessage, e)
      }

    case RegisterExecutorFailed(message) =>
      exitExecutor(1, "Slave registration failed: " + message)

    case LaunchTask(data) =>
      if (executor == null) {
        exitExecutor(1, "Received LaunchTask command but executor was null")
      } else {
        val taskDesc = ser.deserialize[TaskDescription](data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
          taskDesc.name, taskDesc.serializedTask)
      }

    case KillTask(taskId, _, interruptThread) =>
      if (executor == null) {
        exitExecutor(1, "Received KillTask command but executor was null")
      } else {
        executor.killTask(taskId, interruptThread)
      }

    case StopExecutor =>
      stopping.set(true)
      logInfo("Driver commanded a shutdown")
      // Cannot shutdown here because an ack may need to be sent back to the caller. So send
      // a message to self to actually do the shutdown.
      self.send(Shutdown)

    case Shutdown =>
      stopping.set(true)
      new Thread("CoarseGrainedExecutorBackend-stop-executor") {
        override def run(): Unit = {
          // executor.stop() will call `SparkEnv.stop()` which waits until RpcEnv stops totally.
          // However, if `executor.stop()` runs in some thread of RpcEnv, RpcEnv won't be able to
          // stop until `executor.stop()` returns, which becomes a dead-lock (See SPARK-14180).
          // Therefore, we put this line in a new thread.
          executor.stop()
        }
      }.start()
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (stopping.get()) {
      logInfo(s"Driver from $remoteAddress disconnected during shutdown")
    } else if (driver.exists(_.address == remoteAddress)) {
      exitExecutor(1, s"Driver $remoteAddress disassociated! Shutting down.")
    } else {
      logWarning(s"An unknown ($remoteAddress) driver disconnected.")
    }
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    val msg = StatusUpdate(executorId, taskId, state, data)
    driver match {
      case Some(driverRef) => driverRef.send(msg)
      case None => logWarning(s"Drop $msg because has not yet connected to driver")
    }
  }

  /**
   * This function can be overloaded by other child classes to handle
   * executor exits differently. For e.g. when an executor goes down,
   * back-end may not want to take the parent process down.
   */
  protected def exitExecutor(code: Int, reason: String, throwable: Throwable = null) = {
    if (throwable != null) {
      logError(reason, throwable)
    } else {
      logError(reason)
    }
    System.exit(code)
  }
}



    private[spark] object CoarseGrainedExecutorBackend extends Logging {

      private def run(
                       driverUrl: String,
                       executorId: String,
                       hostname: String,
                       cores: Int,
                       appId: String,
                       workerUrl: Option[String],
                       userClassPath: Seq[URL]) {
        // val env0 = SparkEnv.get
        // if(env0 == null) {
        //   createEnv(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath)
        // }
        // SparkHadoopUtil.get.runAsSparkUser { () =>
        var env = SparkEnv.get
        if(env == null) {
          createEnv(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath,
            () => {
              env = SparkEnv.get
              env.rpcEnv.setupEndpoint("Executor", new CoarseGrainedExecutorBackend(
                env.rpcEnv, driverUrl, executorId, hostname, cores, userClassPath, env))
              workerUrl.foreach { url =>
                env.rpcEnv.setupEndpoint("WorkerWatcher", new WorkerWatcher(env.rpcEnv, url))
              }
              env.rpcEnv.awaitTermination()
              SparkHadoopUtil.get.stopExecutorDelegationTokenRenewer()
            })
        } else {
          SparkHadoopUtil.get.runAsSparkUser{ () =>
            env = SparkEnv.get
            env.rpcEnv.setupEndpoint("Executor", new CoarseGrainedExecutorBackend(
              env.rpcEnv, driverUrl, executorId, hostname, cores, userClassPath, env))
            workerUrl.foreach { url =>
              env.rpcEnv.setupEndpoint("WorkerWatcher", new WorkerWatcher(env.rpcEnv, url))
            }
            env.rpcEnv.awaitTermination()
            SparkHadoopUtil.get.stopExecutorDelegationTokenRenewer()
          }
        }

        // }
      }

  private def createEnv(
                         driverUrl: String,
                         executorId: String,
                         hostname: String,
                         cores: Int,
                         appId: String,
                         workerUrl: Option[String],
                         userClassPath: Seq[URL],
                         func: () => Unit) {
    Utils.initDaemon(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(hostname)

      // Bootstrap to fetch the driver's Spark properties.
      val executorConf = new SparkConf
      val port = executorConf.getInt("spark.executor.port", 0)
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        hostname,
        port,
        executorConf,
        new SecurityManager(executorConf),
        clientMode = true)
      val driver = fetcher.setupEndpointRefByURI(driverUrl)
      val props = driver.askWithRetry[Seq[(String, String)]](RetrieveSparkProps) ++
        Seq[(String, String)](("spark.app.id", appId))
      fetcher.shutdown()

      // Create SparkEnv using properties we fetched from the driver.
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }
      if (driverConf.contains("spark.yarn.credentials.file")) {
        logInfo("Will periodically update credentials from: " +
          driverConf.get("spark.yarn.credentials.file"))
        SparkHadoopUtil.get.startExecutorDelegationTokenRenewer(driverConf)
      }
      def run: Unit = func()
    }
  }

  def getProcessUsedMemoryAndCPU(processID: String): (Double, Double) = {
    var memory = 0d;
    var CPU = 0d;
    val NUMCPU = 8;

    val topout = Seq("/bin/sh", " -c ", "\"top -n 1 -b -p " + processID + " | grep " +
       processID + " | tail -1 ").!!.trim.split(" +")

    val len = topout(5).length
    if (topout(5).endsWith("g")) {
        memory = 1024L*1024L*1024L*topout(5).take(len-1).toDouble
    } else if (topout(5).endsWith("m")) {
        memory = 1024L*1024L*topout(5).take(len-1).toDouble
    } else if (topout(5).endsWith("k")) {
        memory = 1024L*topout(5).take(len-1).toDouble
    } else { memory = topout(5).toDouble }

    CPU = topout(8).toDouble / NUMCPU

    return (memory, CPU)
  }

  def main(args: Array[String]) {
    var driverUrl: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit()
      }
    }

    if (driverUrl == null || executorId == null || hostname == null || cores <= 0 ||
      appId == null) {
      printUsageAndExit()
    }

    createEnv(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath, () => {})

    // han sampler 1 begin
    val SAMPLING_PERIOD: Long = 10
    val JMAP_PERIOD: Long = 5000
    val TIMESTAMP_PERIOD: Long = 1000
    var dateFormat: DateFormat = new SimpleDateFormat("hh:mm:ss")

    val system_properties = Utils.getSystemProperties
    val user_home = system_properties.get("user.home")
    var dirname_executor = ""
    user_home match {
      case None =>
        dirname_executor = "/tmp" + "/spark-logs/" + appId + "/" + executorId

      case Some(value) =>
        dirname_executor = value + "/spark-logs/" + appId + "/" + executorId

    }

    logInfo("user home: " + user_home + ", dirname_executor: " + dirname_executor)

    val dir_executor = new File(dirname_executor)
    if (!dir_executor.exists()) {
      dir_executor.mkdirs() }
    val dirname_histo = dirname_executor + "/histo"
    val dir_histo = new File(dirname_histo)
    if (!dir_histo.exists()) {
      dir_histo.mkdirs() }

    val writer = new FileWriter(new File(dirname_executor + "/" + "sparkOutput_worker_" +
       appId + "_" + executorId + ".txt"), true)
    writer.write(appId + "_" + executorId + "\n")
    writer.flush()

    var osBean: com.sun.management.OperatingSystemMXBean =
       ManagementFactory.getPlatformMXBean(classOf[com.sun.management.OperatingSystemMXBean])
    var availableProcessors: Int = osBean.getAvailableProcessors()
    logInfo("No. available processors for executor " + executorId + ": " + availableProcessors)
    var avgUsedCPU: Double = 0
    var numberOfCPUSamples: Long = 1
    var memBean: MemoryMXBean = ManagementFactory.getMemoryMXBean()
    var rtBean: RuntimeMXBean = ManagementFactory.getRuntimeMXBean()
    var mpBeans: List[MemoryPoolMXBean] = ManagementFactory.getMemoryPoolMXBeans()
    var s: String = ""
    mpBeans.asScala.foreach {

      b =>
        s += b.getName() + "\t";
    }
    s += "Used heap\tCommitted heap\tMax heap\tUsed nonheap\tCommitted nonheap\tMax nonheap\t" +
      "Spark offheap\tUsed CPU OSBean\tUsed Exec Memory\tUsed Exec CPU\tUsed DN Memory\t" +
      "Used DN CPU\tUsed NM Memory\tUsed NM CPU\tSpark Storage Memory\tSpark Execution Memory"
    writer.write(s + "\n")
    writer.flush()

    var mBeans: List[MemoryPoolMXBean] = null
    var prevUpTime: Double = 0
    var prevProcessCPUTime: Double = 0
    var upTime: Double = 0
    var processCPUTime: Double = 0

    var processID: String = ""
    var datanodePID: String = ""
    var nodemanagerPID: String = ""

    val pname = ManagementFactory.getRuntimeMXBean().getName()
    processID = pname.substring(0, pname.indexOf('@'))

    val jps = new java.util.Vector[String]()
    jps.add("/bin/bash")
    jps.add("-c")
    jps.add("jps")
    val pbi = new java.lang.ProcessBuilder(jps)
    val pri = pbi.start()
    pri.waitFor()
    if (pri.exitValue()==0) {
      val outReader = new BufferedReader(new InputStreamReader(pri.getInputStream()));
      var source = ""
      source = outReader.readLine()
      while(source != null) {
        try {
          val tokens = source.split(" +")
          if(tokens(1).equals("DataNode")) {
            datanodePID = tokens(0)
          }
          if(tokens(1).equals("NodeManager")) {
            nodemanagerPID = tokens(0)
          }
        } catch { case e: Exception => () }
        finally {
          source = outReader.readLine()
        }
      }
    } else {
      logInfo("Error while getting jps output")
    }
    val ex = new ScheduledThreadPoolExecutor(1)
    ex.setRemoveOnCancelPolicy(true)
    val task = new Runnable {
      var i: Long = 0
      override def run {

        s = ""
        mpBeans.asScala.foreach {
          b =>
            s += b.getUsage().getUsed() + "\t";
        }
        s += memBean.getHeapMemoryUsage().getUsed() + "\t"
        s += memBean.getHeapMemoryUsage().getCommitted() + "\t"
        s += memBean.getHeapMemoryUsage().getMax() + "\t"
        s += memBean.getNonHeapMemoryUsage().getUsed() + "\t"
        s += memBean.getNonHeapMemoryUsage().getCommitted() + "\t"
        s += memBean.getNonHeapMemoryUsage().getMax() + "\t"
        // get used memory from jmap
        // val jmapout = Seq("/bin/sh", "-c", "jmap -histo " + processID + " | tail -1").!!.trim
        // s += jmapout.split(" +")(2)

        // record off heap memory usage
        s += 0 // org.apache.spark.unsafe.Platform.TOTAL_BYTES;

        upTime = rtBean.getUptime() * 10000
        processCPUTime = osBean.getProcessCpuTime()
        var elapsedCPU: Double = processCPUTime - prevProcessCPUTime
        var elapsedTime: Double = upTime - prevUpTime
        var usedCPU: Double = 0
        if (elapsedTime > 0.0) {
          usedCPU = math.min(99.0, elapsedCPU / (elapsedTime * availableProcessors))
          avgUsedCPU += usedCPU
          numberOfCPUSamples += 1
        }
        s += "\t" + usedCPU.toString()
        prevUpTime = upTime
        prevProcessCPUTime = processCPUTime

        var res: (Double, Double) = (0, 0);
        try { res = getProcessUsedMemoryAndCPU(processID); s += "\t" + res._1 + "\t" + res._2 }
          catch { case e: Exception => e.printStackTrace() }

        try { res = getProcessUsedMemoryAndCPU(datanodePID); s += "\t" + res._1 + "\t" + res._2 }
          catch { case e: Exception => e.printStackTrace() }

        try { res = getProcessUsedMemoryAndCPU(nodemanagerPID); s+="\t" + res._1 + "\t" + res._2 }
          catch { case e: Exception => e.printStackTrace() }

        // Get storage and execution memory usage
        // try {
        //  val env = SparkEnv.get
        //  val mm = env.memoryManager
        //  s += "\t" + mm.storageMemoryUsed
        //  s += "\t" + mm.executionMemoryUsed
        // } catch{ case e:Exception => e.printStackTrace()
        //  s += "\t" + 0
        //  s += "\t" + 0
        // }
        var env = SparkEnv.get
        if(env == null) {
          createEnv(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath,
            () => {
                env = SparkEnv.get
                val mm = env.memoryManager
                s += "\t" + mm.storageMemoryUsed
                s += "\t" + mm.executionMemoryUsed
            })
        } else {
            val mm = env.memoryManager
            s += "\t" + mm.storageMemoryUsed
            s += "\t" + mm.executionMemoryUsed
        }

        if (i % TIMESTAMP_PERIOD == 0) {
          var time: String = dateFormat.format(new Date())
          s += "\t" + time
        }

        /*
        if (i % JMAP_PERIOD == 0) {
          var time: String = dateFormat.format(new Date())
          val pname = ManagementFactory.getRuntimeMXBean().getName()
          val pid = pname.substring(0, pname.indexOf('@'))
          val command = "jmap -histo " + pid
          val result = command.!!
          val writer1 = new FileWriter(new File(dirname_histo + "/" + "sparkOutput_worker_" +
             appId + "_" + executorId + "_" + time + ".txt"), true)
          writer1.write(result)
          writer1.flush()
          writer1.close()
        }
        */

        i = i + SAMPLING_PERIOD
        writer.write(s + "\n")
        writer.flush()

      }
    }
    val f = ex.scheduleAtFixedRate(task, 0, SAMPLING_PERIOD, TimeUnit.MILLISECONDS)
    // han sampler 1 end

    run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath)

    // han sampler 2 begin
    // avgUsedCPU /= numberOfCPUSamples
    // logInfo("Average used CPU of executor " + executorId + ": " + avgUsedCPU)

    f.cancel(true)
    writer.flush()
    writer.close()



  }

  private def printUsageAndExit() = {
    // scalastyle:off println
    System.err.println(
      """
      |Usage: CoarseGrainedExecutorBackend [options]
      |
      | Options are:
      |   --driver-url <driverUrl>
      |   --executor-id <executorId>
      |   --hostname <hostname>
      |   --cores <cores>
      |   --app-id <appid>
      |   --worker-url <workerUrl>
      |   --user-class-path <url>
      |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }

}
