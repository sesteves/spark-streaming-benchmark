import scala.annotation.tailrec

import java.io.OutputStream
import java.util.concurrent.TimeUnit._

class DynamicRateLimitedOutputStream(out: OutputStream, minBytesPerSec: Int, maxBytesPerSec: Int,
                                     stepBytes: Int, stepDuration: Int)
  extends OutputStream {

  require(minBytesPerSec > 0)
  require(maxBytesPerSec >= minBytesPerSec)

  private val SYNC_INTERVAL = NANOSECONDS.convert(10, SECONDS)
  private val CHUNK_SIZE = 8192
  private var lastSyncTime = System.nanoTime
  private var bytesWrittenSinceSync = 0L
  private var currentBytesPerSec = minBytesPerSec
  private var duration = -1l
  private var lastTick = -1l
  private var isIncreasing = true

  override def write(b: Int) {
    waitToWrite(1)
    out.write(b)
  }

  override def write(bytes: Array[Byte]) {
    val tick = System.currentTimeMillis()
    if(lastTick == -1) {
      lastTick = tick
    }
    duration += tick - lastTick
    lastTick = tick

    if(duration > stepDuration) {
      if(isIncreasing) {
        if (currentBytesPerSec + stepBytes > maxBytesPerSec) {
          currentBytesPerSec = maxBytesPerSec - (currentBytesPerSec + stepBytes - maxBytesPerSec)
          isIncreasing = false
        } else {
          currentBytesPerSec += stepBytes
        }
      } else {
        if(currentBytesPerSec - stepBytes < minBytesPerSec) {
          currentBytesPerSec = minBytesPerSec + (minBytesPerSec - (currentBytesPerSec - stepBytes))
          isIncreasing = true
        } else {
          currentBytesPerSec -= stepBytes
        }
      }

      duration = 0
    }

    write(bytes, 0, bytes.length)
  }

  @tailrec
  override final def write(bytes: Array[Byte], offset: Int, length: Int) {
    val writeSize = math.min(length - offset, CHUNK_SIZE)
    if (writeSize > 0) {
      waitToWrite(writeSize)
      out.write(bytes, offset, writeSize)
      write(bytes, offset + writeSize, length)
    }
  }

  override def flush() {
    out.flush()
  }

  override def close() {
    out.close()
  }

  @tailrec
  private def waitToWrite(numBytes: Int) {
    val now = System.nanoTime
    val elapsedNanosecs = math.max(now - lastSyncTime, 1)
    val rate = bytesWrittenSinceSync.toDouble * 1000000000 / elapsedNanosecs

    if (rate < currentBytesPerSec) {
      // It's okay to write; just update some variables and return
      bytesWrittenSinceSync += numBytes
      if (now > lastSyncTime + SYNC_INTERVAL) {
        println(s"Rate = $rate bytes/sec")
        // Sync interval has passed; let's resync
        lastSyncTime = now
        bytesWrittenSinceSync = numBytes
      }
    } else {
      // Calculate how much time we should sleep to bring ourselves to the desired rate.
      val targetTimeInMillis = bytesWrittenSinceSync * 1000 / currentBytesPerSec
      val elapsedTimeInMillis = elapsedNanosecs / 1000000
      val sleepTimeInMillis = targetTimeInMillis - elapsedTimeInMillis
      if (sleepTimeInMillis > 0) {
        // logTrace("Natural rate is " + rate + " per second but desired rate is " +
        // desiredBytesPerSec + ", sleeping for " + sleepTimeInMillis + " ms to compensate.")
        Thread.sleep(sleepTimeInMillis)
      }
      waitToWrite(numBytes)
    }
  }
}

