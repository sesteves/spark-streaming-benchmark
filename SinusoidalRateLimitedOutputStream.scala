import java.io.OutputStream

/**
 * Created by sesteves on 24-08-2015.
 */

class SinusoidalRateLimitedOutputStream(out: OutputStream, tendency: String, minBytesPerSec: Int, maxBytesPerSec: Int,
                                     stepDuration: Int, stepBytes: Int)
  extends DynamicRateLimitedOutputStream {

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

}