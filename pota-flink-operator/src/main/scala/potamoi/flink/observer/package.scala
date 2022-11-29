package potamoi.flink

import potamoi.flink.share.FlinkOprErr
import potamoi.syntax._
import zio.ZIO.logError
import zio.{Duration, IO, Ref, Schedule}

package object observer {

  /**
   * Cyclic trigger effect and recording of the first non-repeating error.
   */
  @inline private[observer] def loopTrigger[E <: FlinkOprErr, A](spaced: Duration)(effect: IO[E, A]): IO[E, Unit] = for {
    preErr <- Ref.make[Option[FlinkOprErr]](None)
    loopEffect <- effect
      .schedule(Schedule.spaced(spaced))
      .tapError { err =>
        preErr.get.flatMap { pre =>
          (logError(err.toPrettyStr) *> preErr.set(err)).when(!pre.contains(err))
        }
      }
      .ignore
      .forever
  } yield loopEffect
}
