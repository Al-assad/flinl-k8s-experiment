package potamoi.flink

import potamoi.syntax._
import zio.ZIO.logError
import zio.{Duration, IO, Ref, Schedule}

package object observer {

  /**
   * Cyclic trigger effect and recording of the first non-repeating error.
   */
  @inline private[observer] def loopTrigger[E, A](spaced: Duration)(effect: IO[E, A]): IO[E, Unit] = for {
    preErr <- Ref.make[Option[E]](None)
    loopEffect <- effect
      .tapError { err =>
        preErr.get.flatMap { pre =>
          (logError(toPrettyString(err)) *> preErr.set(err)).when(!pre.contains(err))
        }
      }
      .ignore
      .schedule(Schedule.spaced(spaced))
      .forever
  } yield loopEffect
}
