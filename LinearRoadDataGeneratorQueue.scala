import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * Created by Sergio on 18/09/2015.
 */
object LinearRoadDataGeneratorQueue {

  def apply(ssc: StreamingContext, queue: mutable.SynchronizedQueue[RDD[String]], file: String, sleepMillis: Int): Unit = {

    var count = 0
    var list = ListBuffer.empty[String]
    var startTimestamp = -1
    while(true) {
      for (line <- Source.fromFile(file).getLines()) {
        val ts = line.substring(2, line.indexOf(',',2)).toInt
        if(startTimestamp < 0)
          startTimestamp = ts

        if(ts - startTimestamp <= 30) {
          list += line
          count += 1
        } else {
          queue += ssc.sparkContext.makeRDD(list)
          list = ListBuffer(line)
          println(s"Emmited reports: $count")
          println(s"Queue size: ${queue.size}")
          count = 0
          startTimestamp = ts
          Thread.sleep(sleepMillis)
        }
      }
    }
  }
}
