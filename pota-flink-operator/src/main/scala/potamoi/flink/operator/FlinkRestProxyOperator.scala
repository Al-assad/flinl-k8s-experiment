package potamoi.flink.operator

import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.common.ActorInteropException
import potamoi.config.PotaConf
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.share.FlinkIO
import potamoi.flink.share.FlinkOprErr.NeedToTrackClusterFirst
import potamoi.flink.share.cache.FlinkRestProxyCache
import potamoi.flink.share.model.Fcid
import potamoi.timex._
import zio.ZIO
import zio.ZIO.fail

/**
 * Flink rest endpoint reverse proxy operator.
 */
trait FlinkRestProxyOperator {

  /**
   * Enable proxying the rest server of the target flink cluster to revise service.
   */
  def enable(fcid: Fcid): FlinkIO[Unit]

  /**
   * Disable proxying the rest server of the target flink cluster.
   */
  def disable(fcid: Fcid): FlinkIO[Unit]

  /**
   * Listing the proxying flink cluster.
   */
  def list: FlinkIO[Set[Fcid]]
}

object FlinkRestProxyOperator {

  def live(potaConf: PotaConf, guardian: ActorGuardian, flinkObserver: FlinkObserver): ZIO[Any, ActorInteropException, FlinkRestProxyOperator] =
    for {
      restProxyCache <- guardian.spawn(FlinkRestProxyCache(potaConf.akka.ddata.getFlinkRestProxy), "flkRestProxyCache")
      askTimeout = potaConf.akka.defaultAskTimeout
      sc         = guardian.scheduler
    } yield Live(restProxyCache, flinkObserver)(sc, askTimeout)

  case class Live(restProxySet: ActorRef[FlinkRestProxyCache.Cmd], flinkObserver: FlinkObserver)(implicit sc: Scheduler, askTimeout: Timeout)
      extends FlinkRestProxyOperator {

    override def enable(fcid: Fcid): FlinkIO[Unit] =
      flinkObserver.manager.existTrackedCluster(fcid).flatMap {
        case false => fail(NeedToTrackClusterFirst(fcid))
        case true  => restProxySet.put(fcid)
      }
    override def disable(fcid: Fcid): FlinkIO[Unit] = restProxySet.remove(fcid)
    override def list: FlinkIO[Set[Fcid]]           = restProxySet.list
  }
}
