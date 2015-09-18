import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * Created by Sergio on 18/09/2015.
 */
object LinearRoadDataGenerator {

  def apply(ssc: StreamingContext, queue: mutable.SynchronizedQueue[RDD[String]], file: String, sleepMillis: Int): Unit = {

    var count = 0
    var list = ListBuffer.empty[String]
    while(true) {
      for (line <- Source.fromFile(file).getLines()) {
        list += line
        count += 1
        if(count % 1000 == 0) {
          queue += ssc.sparkContext.makeRDD(list)
          list = ListBuffer.empty[String]
          Thread.sleep(sleepMillis)
        }
      }
    }
  }
}
