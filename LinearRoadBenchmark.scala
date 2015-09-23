import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, Duration, StreamingContext}

import scala.collection.mutable

/**
 * Created by Sergio on 17/09/2015.
 */
object LinearRoadBenchmark {

  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.println("Usage: LinearRoadBenchmark <numStreams> <host> <port> <batchMillis> <windowSec> <file>")
      System.exit(1)
    }

    val (numStreams, host, port, batchMillis, windowSec, file) =
      (args(0).toInt, args(1), args(2).toInt, args(3).toInt, args(4).toInt, args(5))
    val sparkConf = new SparkConf()

    sparkConf.set("spark.art.window.duration", (windowSec * 1000).toString)
    sparkConf.set("spark.akka.heartbeat.interval", "100")

    sparkConf.setAppName("LinearRoadBenchmark")
    sparkConf.setJars(Array("target/scala-2.10/benchmark-app_2.10-0.1-SNAPSHOT.jar"))
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.executor.extraJavaOptions", " -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC " +
      "-XX:+AggressiveOpts -XX:FreqInlineSize=300 -XX:MaxInlineSize=300 ")
    if (sparkConf.getOption("spark.master") == None) {
      sparkConf.setMaster("local[*]")
    }

    // val queue =  new mutable.SynchronizedQueue[RDD[String]]()

    // The batch interval for this workload should be 5 seconds
    val ssc = new StreamingContext(sparkConf, Duration(batchMillis))
    // val stream = ssc.queueStream(queue)

    val carPosFreq = ssc.sparkContext.accumulableCollection(mutable.HashMap.empty[Int, ((Int, Int, Int, Int),Int)])

    val rawStreams = (1 to numStreams).map(_ =>
      ssc.socketTextStream(host, port, StorageLevel.MEMORY_ONLY_SER)).toArray
    val stream = ssc.union(rawStreams)


    val MaxSegment = 100
    val MaxSpeed: Double = 100
    // val vehicles = HashMap.empty[Int, Vehicle]
    //val segments = HashMap.empty[Int, Segment]

    // val toll = 2 * Math.pow((numberOfCars - 50), 2);
    // val congestion = numberOfCars * Math.pow(MAX_SPEED / avgSpeed, 2 * (accident + 1));

    val vehicleReports = stream.flatMap(_.split(' ')).filter(_.startsWith("0")).map(line => {
      val items = line.split(",").map(_.toInt)
      (items(1), items(2), items(3), items(4), items(5), items(6), items(7), items(8))
    })

    val accidents = vehicleReports.map({ case (time, carId, speed, xway, lane, dir, seg, pos) =>
      val absoluteSegment = xway * MaxSegment + seg
      val location = (absoluteSegment, lane, dir, pos)

      val isCarStopped = {
        // check if car is stopped
        val previous = carPosFreq.localValue.get(carId).getOrElse((location, 0))
        println("CarId: " + carId + "Previous location: " + previous._1 + ", freq: " + previous._2)
        if (previous._1 == location) {
          if (previous._2 == 3) {
            true
          } else {
            carPosFreq += (carId ->(location, previous._2 + 1))
            false
          }
        } else {
          carPosFreq += (carId ->(location, 1))
          false
        }
      }
      println("Is car stopped: " + isCarStopped)
      (location, if(isCarStopped) 1 else 0)
    }).reduceByKey(_ + _).map({case (location, stoppedCars) =>
      (location._1, if(stoppedCars > 1) 1 else 0)
    }).reduceByKey((a,b) => if(a == 1 || b == 1) 1 else 0)


    val averageSpeedAndNoOfCars = vehicleReports.map({ case (time, carId, speed, xway, lane, dir, seg, pos) =>
      val absoluteSegment = xway * MaxSegment + seg
      (absoluteSegment, (speed, 1))
    }).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
      .map({ case (segment, (speedSum, numberOfCars )) =>
      (segment, (speedSum.toDouble / numberOfCars.toDouble, numberOfCars))
    })


    averageSpeedAndNoOfCars.join(accidents).map({ case (segment, ((averageSpeed, numberOfCars), accident)) =>
      println("Accident: " + accident)
      val congestion = numberOfCars * math.pow(MaxSpeed / averageSpeed, 2 * (accident + 1))
      (congestion / 50, 1)
    }).reduceByWindow((a,b) => (a._1 + b._1, a._2 + b._2),
        Seconds(windowSec.toInt), Seconds(windowSec.toInt))
      .foreachRDD(rdd=>{
      val count = rdd.count
      println(s"Number of records: $count")
      if(count > 0) {
        val classification = rdd.first()
        println(s"Average: ${classification._1 / classification._2}")
      }
    })


//    distFile.filter(_.startsWith("0")).map(line => {
//      val items = line.split(",").map(_.toInt)
//      val vehicle = Vehicle(items(2), items(3), items(4), items(5), items(6), items(7), items(9))
//      // vehicles += (vehicle.carId -> vehicle)
//
//      val absoluteSegment = vehicle.xway * MaxSegment + vehicle.seg
//
//      // FIXEME cars that leave segments
//      if(segments.contains(absoluteSegment)) {
//        segments(absoluteSegment).addVehicle(vehicle.speed)
//      } else {
//        segments += (absoluteSegment -> Segment(vehicle.speed, 1, false))
//      }
//
//      segments.values.map(segment => {
//        val congestion = segment.numberOfCars * math.pow(MaxSpeed / segment.getAverageSpeed,
//          2 * ((if (segment.accident) 1 else 0) + 1))
//        (congestion / 50.0, 1)
//      })
//    })


    ssc.start
    // LinearRoadDataGeneratorQueue(ssc, queue, file, 3000)
    ssc.awaitTermination
  }

}
