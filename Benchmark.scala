import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.util.IntParam

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

  def fib2( n : BigInt ) : BigInt = {
    var a = 0
    var b = 1
    var i = 0

    while( i < n ) {
      val c = a + b
      a = b
      b = c
      i = i + 1
    }
    return a
  }

  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.println("Usage: RawNetworkGrep <numStreams> <host> <port> <batchMillis> <cores> <filter>")
      System.exit(1)
    }

    val (numStreams, host, port, batchMillis, cores, filter) = (args(0).toInt, args(1), args(2).toInt, args(3).toInt, args(4), args(5))
    val sparkConf = new SparkConf()
    // sparkConf.setMaster("spark://ginja-A1:7077")
    sparkConf.setAppName("BenchMark")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.executor.extraJavaOptions", " -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+AggressiveOpts -XX:FreqInlineSize=300 -XX:MaxInlineSize=300 ")
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
    // union.count().map(c => s"Received $c records").print()


    union.repartition(cores.toInt).filter(line => Random.nextInt(filter.toInt) == 0).map(line => {
      var sum = BigInt(0)
      line.toCharArray.foreach(chr => sum += fib2(BigInt(chr.toInt).pow(1)))
      sum
    }).reduceByWindow(_+_, Seconds(1),Seconds(1)).map(s => s"### result: $s").print()

//      .foreachRDD(rdd => {
//      val startTick = System.currentTimeMillis()
//      val result = rdd.take(1)
//      val timeDiff = System.currentTimeMillis() - startTick
//
//      println("### Result array size: " + result.size)
//      println("### Time taken: %d".format(timeDiff))
//    })


    ssc.start()
    ssc.awaitTermination()
  }
}

