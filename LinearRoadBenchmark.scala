import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.collection.mutable.HashMap

/**
 * Created by Sergio on 17/09/2015.
 */
object LinearRoadBenchmark {

  def main(args: Array[String]) {
    if (args.length != 8) {
      System.err.println("Usage: LinearRoadBenchmark <numStreams> <host> <port> <batchMillis> <windowSec> <file>")
      System.exit(1)
    }

    val (numStreams, host, port, batchMillis, cores, filter, windowSec, file) =
      (args(0).toInt, args(1), args(2).toInt, args(3).toInt, args(4), args(5), args(6), args(7))
    val sparkConf = new SparkConf()

    sparkConf.set("spark.art.window.duration", (windowSec.toInt * 1000).toString)
    sparkConf.set("spark.akka.heartbeat.interval", "100")

    sparkConf.setAppName("Benchmark")
    sparkConf.setJars(Array("target/scala-2.10/benchmark-app_2.10-0.1-SNAPSHOT.jar"))
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.executor.extraJavaOptions", " -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC " +
      "-XX:+AggressiveOpts -XX:FreqInlineSize=300 -XX:MaxInlineSize=300 ")
    if (sparkConf.getOption("spark.master") == None) {
      sparkConf.setMaster("local[*]")
    }
    // sparkConf.set("spark.cores.max", cores)

    // Create the context
    val ssc = new StreamingContext(sparkConf, Duration(batchMillis))
    val distFile = ssc.textFileStream(file)


    val MaxSegment = 100
    val vehicles = HashMap.empty[Int, Vehicle]
    val segments = HashMap.empty[Int, Segment]

    // https://gist.github.com/tpolecat/95c974d72528252874a3



    distFile.filter(_.startsWith("0")).map(_.split(",").map(_.toInt))


    distFile.filter(_.startsWith("0")).map(line => {

      val items = line.split(",").map(_.toInt)
      val vehicle = Vehicle(items(2), items(3), items(4), items(5), items(6), items(7), items(9))
      vehicles += (vehicle.carId -> vehicle)

      val absoluteSegment = vehicle.xway * MaxSegment + vehicle.seg

      if(segments.contains(absoluteSegment)) {
        segments(absoluteSegment).addVehicle(vehicle.speed)
      } else {
        segments += (absoluteSegment -> Segment(vehicle.speed, 1, false))
      }



    })





  }

  case class Vehicle(carId: Int, speed: Int, xway: Int, lane: Int, dir: Int, seg: Int, pos: Int)

  case class Segment(speedSum: Int, numberOfCars: Int, accident: Boolean) {
    def addVehicle(speed: Int): Unit = {
      speedSum += speed
      numberOfCars += 1
    }
  }
}
