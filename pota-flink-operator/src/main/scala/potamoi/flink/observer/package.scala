package potamoi.flink

import potamoi.actorx._
import potamoi.common.Syntax.GenericPF
import potamoi.flink.share.FlinkOprErr
import potamoi.flink.share.model.Fcid
import potamoi.syntax._
import zio.ZIO.logError
import zio.{Duration, IO, Ref, Schedule}

package object observer {

  // marshall/unmarshall between fcid and cluster-sharding entity id.
  @inline def marshallFcid(fcid: Fcid): String   = s"${fcid.clusterId}@${fcid.namespace}"
  @inline def unMarshallFcid(fcid: String): Fcid = fcid.split('@').contra { arr => Fcid(arr(0), arr(1)) }

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
