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
            out.println(line)
            for(xway <- 2 to lFactor) {
              val newLine = line.replaceFirst("(^-?[0-9]+,-?[0-9]+,-?[0-9]+,-?[0-9]+,)-?[0-9]+(,.*)",
                "$1" + xway + "$2")
              out.println(newLine)
            }
            count += 1
          } else {
            println(s"Emmited reports: $count")
            count = 0
            out.flush()
            startTimestamp = ts
            Thread.sleep(sleepMillis)
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
