import java.io.OutputStream

/**
 * Created by sesteves on 24-08-2015.
 */

class StepRateLimitedOutputStream(out: OutputStream, tendency: String, minBytesPerSec: Int, maxBytesPerSec: Int,
                                        stepDuration: Int)
  extends DynamicRateLimitedOutputStream(out, tendency, minBytesPerSec, maxBytesPerSec, stepDuration) {

  private var duration = -1l
  private var lastTick = -1l

  override def write(bytes: Array[Byte]) {
    val tick = System.currentTimeMillis()
    if (lastTick == -1) {
      lastTick = tick
    }
    duration += tick - lastTick
    lastTick = tick

    if (duration > stepDuration) {

      if (currentBytesPerSec == minBytesPerSec) {
        currentBytesPerSec = maxBytesPerSec
      } else {
        currentBytesPerSec = minBytesPerSec
      }
      duration = 0
    }

    write(bytes, 0, bytes.length)
  }

}