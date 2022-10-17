package kce.common

import java.time.{Duration => JDuration}
import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

object TimeExtension {

  implicit def scalaDuration2JavaDuration(duration: FiniteDuration): JDuration = JDuration.ofMillis(duration.toMillis)

  implicit class FiniteDurationWrapper(duration: FiniteDuration) {
    def asJava: JDuration = scalaDuration2JavaDuration(duration)
  }

}
