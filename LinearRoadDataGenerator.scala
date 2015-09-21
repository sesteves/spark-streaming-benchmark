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
    if (args.length != 3) {
      System.err.println("Usage: RawTextSender <port> <file> <sleepMillis>")
      System.exit(1)
    }
    // Parse the arguments using a pattern match
    val (port, file, sleepMillis) = (args(0).toInt, args(1), args(2).toInt)

    val serverSocket = new ServerSocket(port)
    println("Listening on port " + port)


    val ser = new KryoSerializer(new SparkConf()).newInstance()

    while (true) {
      val socket = serverSocket.accept()
      println("Got a new connection")

      val buffer = new ByteArrayOutputStream()
      //val buffer = new PrintWriter(socket.getOutputStream)
      val serStream = ser.serializeStream(buffer)
      val out = socket.getOutputStream
      try {
        var count = 0
        var startTimestamp = -1
        for (line <- Source.fromFile(file).getLines()) {
          val ts = line.substring(2, line.indexOf(',',2)).toInt
          if(startTimestamp < 0)
            startTimestamp = ts

          if(ts - startTimestamp <= 30) {
            serStream.writeObject(line)
            // out.write(line)
            out.write(buffer.toByteArray)
            count += 1
          } else {
            println(s"Emmited reports: $count")
            count = 0
            out.flush()
            startTimestamp = ts
            Thread.sleep(sleepMillis)
          }
        }
      } catch {
        case e: IOException =>
          println("Client disconnected")
          socket.close()
      }
    }

}

}
