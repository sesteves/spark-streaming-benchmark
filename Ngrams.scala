import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.util.Random

/**
 * Created by Sergio on 14/05/2015.
 */


object Ngrams {


  def main(args: Array[String]): Unit = {
    if (args.length != 9) {
      System.err.println("Usage: RawNetworkGrep <numStreams> <host> <port> <batchMillis> <cores> <filter> <windowSec> <operation> <n>")
      System.exit(1)
    }

    val (numStreams, host, port, batchMillis, cores, filter, windowSec, operation, n) =
      (args(0).toInt, args(1), args(2).toInt, args(3).toInt, args(4), args(5), args(6), args(7), args(8))
    val sparkConf = new SparkConf()

    sparkConf.set("spark.art.window.duration", (windowSec.toInt * 1000).toString)

    sparkConf.setMaster("yarn-client")
    sparkConf.set("spark.yarn.am.memory", "512m")
    sparkConf.set("spark.executor.instances", "2") // The number of executors. Note that this property is incompatible with spark.dynamicAllocation.enabled.

    sparkConf.setAppName("Ngrams")
    sparkConf.setJars(Array("target/scala-2.10/benchmark-app_2.10-0.1-SNAPSHOT.jar"))
    sparkConf.set("spark.executor.memory", "4g")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.executor.extraJavaOptions", " -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC " +
      "-XX:+AggressiveOpts -XX:FreqInlineSize=300 -XX:MaxInlineSize=300 ")
    if (sparkConf.getOption("spark.master") == None) {
      sparkConf.setMaster("local[*]")
    }
    sparkConf.set("spark.cores.max", cores)

    val ssc = new StreamingContext(sparkConf, Duration(batchMillis))
    val rawStreams = (1 to numStreams).map(_ =>
      ssc.rawSocketStream[String](host, port, StorageLevel.MEMORY_ONLY_SER)).toArray
    val union = ssc.union(rawStreams)

    val lines = union.filter((line) => Random.nextInt(10) < filter.toInt)

    lines.flatMap(line => {
      val words = line.split(' ')
      var ngrams = Traversable.empty[String]
      (2 to n.toInt).foreach(i => ngrams = ngrams ++ words.sliding(i).map(_.mkString(" ")).toTraversable)
      ngrams
    }).map((_, 1))
      .reduceByKeyAndWindow(((_: Int) + (_: Int)), Seconds(windowSec.toInt), Seconds(windowSec.toInt))
      .foreachRDD(rdd => {
        val topList = rdd.take(10)
        topList.foreach { case (ngram, count) => println("### %s (%s times)".format(ngram, count)) }
      })

    ssc.start()
    ssc.awaitTermination()
  }
}