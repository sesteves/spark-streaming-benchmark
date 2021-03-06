import java.io.{ObjectOutputStream, PrintWriter, IOException, ByteArrayOutputStream}
import java.net.ServerSocket
import java.nio.ByteBuffer

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

import scala.io.Source

/**
 * Created by Sergio on 19/09/2015.
 */
object LinearRoadDataGenerator {

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("Usage: RawTextSender <port> <file> <l-factor> <sleepMillis>")
      System.exit(1)
    }
    // Parse the arguments using a pattern match
    val (port, file, lFactor, sleepMillis) = (args(0).toInt, args(1), args(2).toInt, args(3).toInt)

    val serverSocket = new ServerSocket(port)
    println("Listening on port " + port)

    val lines = Source.fromFile(file).getLines().toArray
    val bigLine = new StringBuilder

    while (true) {
      val socket = serverSocket.accept()
      println("Got a new connection")

      val out = new PrintWriter(socket.getOutputStream)
      try {
        var count = 0
        var startTimestamp = -1
        // for (line <- Source.fromFile(file).getLines()) {
        for(line <- lines) {
          val ts = line.substring(2, line.indexOf(',',2)).toInt
          if(startTimestamp < 0)
            startTimestamp = ts

          if(ts - startTimestamp <= 30) {
            val items = line.split(',')
            val firstPart = items.take(3).mkString("", ",", ",")
            val middlePart = "," + items(3) + ","
            val lastPart = items.drop(5).mkString(",", ",", " ")

//            val lineMask = line.replaceFirst("(^-?[0-9]+,-?[0-9]+,-?[0-9]+)(,-?[0-9]+,)-?[0-9]+(,.*)",
//              "$1%s$2%s$3 ") // the last space is needed

            for(xway <- 1 to lFactor) {
//              bigLine.append(lineMask.format("%03d".format(xway), xway))
              bigLine.append(firstPart + "%03d".format(xway) + middlePart + xway + lastPart)
            }
            count += lFactor
          } else {
            println(s"Emmited reports: $count")
            count = 0
            out.println(bigLine.toString)
            bigLine.clear
            startTimestamp = ts
            // Thread.sleep(sleepMillis)
          }
        }
        println("Input file have been totally consumed.")
      } catch {
        case e: IOException =>
          println("Client disconnected")
          socket.close()
      }
    }
  }
}
