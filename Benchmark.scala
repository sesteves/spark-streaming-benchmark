import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.util.IntParam
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD

import scala.collection.mutable.HashMap
import scala.util.Random

/**
 * Receives text from multiple rawNetworkStreams and counts how many '\n' delimited
 * lines have the word 'the' in them. This is useful for benchmarking purposes. This
 * will only work with spark.streaming.util.RawTextSender running on all worker nodes
 * and with Spark using Kryo serialization (set Java property "spark.serializer" to
 * "org.apache.spark.serializer.KryoSerializer").
 * Usage: RawNetworkGrep <numStreams> <host> <port> <batchMillis>
 *   <numStream> is the number rawNetworkStreams, which should be same as number
 *               of work nodes in the cluster
 *   <host> is "localhost".
 *   <port> is the port on which RawTextSender is running in the worker nodes.
 *   <batchMillise> is the Spark Streaming batch duration in milliseconds.
 */
object Benchmark {

  def main(args: Array[String]) {
    if (args.length != 8) {
      System.err.println("Usage: Benchmark <numStreams> <host> <port> <batchMillis> <cores> <filter> <operation> <windowSec>")
      System.exit(1)
    }

    val (numStreams, host, port, batchMillis, cores, filter, operation, windowSec) =
      (args(0).toInt, args(1), args(2).toInt, args(3).toInt, args(4), args(5), args(6), args(7))
    val sparkConf = new SparkConf()

    sparkConf.set("spark.art.window.duration", (windowSec.toInt * 1000).toString)
    sparkConf.set("spark.akka.heartbeat.interval", "100")

    sparkConf.setMaster("spark://ginja-A1:7077")
    sparkConf.setAppName("Benchmark")
    sparkConf.setJars(Array("target/scala-2.10/benchmark-app_2.10-0.1-SNAPSHOT.jar"))
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.executor.extraJavaOptions", " -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC " +
      "-XX:+AggressiveOpts -XX:FreqInlineSize=300 -XX:MaxInlineSize=300 ")
    if (sparkConf.getOption("spark.master") == None) {
      // Master not set, as this was not launched through Spark-submit. Setting master as local."
      sparkConf.setMaster("local[*]")
    }
    sparkConf.set("spark.cores.max", cores)

    // Create the context
    val ssc = new StreamingContext(sparkConf, Duration(batchMillis))

    val rawStreams = (1 to numStreams).map(_ =>
      ssc.rawSocketStream[String](host, port, StorageLevel.MEMORY_ONLY_SER)).toArray
    val union = ssc.union(rawStreams)


    if("filter".equals(operation)) {

      // val map = HashMap.empty[String,Int]
      // val words = union.flatMap(_.split(' ')).foreachRDD()




    }

    if("reduce".equals(operation)) {



    }

    if("join".equals(operation)) {



    }

    if("stdev".equals(operation)) {

//      val wordCharValues = words.map(word => {
//        var sum = 0
//        word.toCharArray.foreach(c => {sum += c.toInt; }) // fib2(c.toInt)
//        val value = sum.toDouble / word.length.toDouble
//        val average = 1892.162961
//
//
//        (math.pow(value - average, 2), 1)
//      })
//        .reduceByWindow({ case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)},
//          Seconds(windowSec.toInt), Seconds(windowSec.toInt))
//
//      wordCharValues.foreachRDD(rdd => {
//        val startTick = System.currentTimeMillis()
//        val result = rdd.take(1)
//        val timeDiff = System.currentTimeMillis() - startTick
//
//        val topList = rdd.take(10)
//
//        println("### Result array size: " + result.size)
//        if(result.size > 0)
//          println("### STDEV: %f".format(math.sqrt(result(0)._1.toDouble / result(0)._2.toDouble)))
//
//        topList.foreach(println)
//        println("### Time taken: %d".format(timeDiff))
//      })

    }




    ssc.start()
    ssc.awaitTermination()
  }
}

