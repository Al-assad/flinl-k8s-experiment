package potamoi.common

import akka.util.Timeout
import zio.{Duration => ZioDuration}

import java.time.{Duration => JDuration}
import scala.concurrent.duration.{Duration => ScalaDuration, FiniteDuration, NANOSECONDS}
import scala.language.implicitConversions

/**
 * Time, Duration extension.
 */
object TimeExtension {

  @inline implicit def scalaToJava(duration: FiniteDuration): JDuration = JDuration.ofNanos(duration.toNanos)

  @inline implicit def scalaToTimeout(duration: ScalaDuration): Timeout = Timeout(duration.toNanos, NANOSECONDS)
  @inline implicit def scalaToZIO(scalaDuration: ScalaDuration): zio.Duration = scalaDuration match {
    case ScalaDuration.Inf  => ZioDuration.Infinity
    case ScalaDuration.Zero => ZioDuration.Zero
    case d                  => ZioDuration.fromNanos(d.toNanos)
  }

  @inline implicit def zioToTimeout(duration: ZioDuration): Timeout = Timeout(duration.toNanos, NANOSECONDS)
  @inline implicit def zioToScala(zioDuration: ZioDuration): ScalaDuration = zioDuration match {
    case ZioDuration.Infinity => ScalaDuration.Inf
    case ZioDuration.Zero     => ScalaDuration.Zero
    case d                    => ScalaDuration.fromNanos(d.toNanos)
  }
}
