package potamoi.flink.observer

import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy.restart
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.cluster.ddata.typed.scaladsl.Replicator.{ReadLocal, WriteLocal}
import akka.util.Timeout
import potamoi.cluster.LWWMapDData
import potamoi.common.ActorExtension.BehaviorWrapper
import potamoi.conf.AkkaConf
import potamoi.flink.share.{Fcid, FlinkRestSvcEndpoint}

/**
 * Flink cluster svc rest endpoint distributed data storage base on LWWMap.
 */
object ClusterRestEndpointDData extends LWWMapDData[Fcid, FlinkRestSvcEndpoint] {

  val cacheId    = "flink-rest-endpoint"
  val init       = LWWMap.empty[Fcid, FlinkRestSvcEndpoint]
  val writeLevel = WriteLocal
  val readLevel  = ReadLocal

  def apply(akkaConf: AkkaConf): Behavior[Cmd] =
    Behaviors.setup { implicit ctx =>
      implicit val node             = DistributedData(ctx.system).selfUniqueAddress
      implicit val timeout: Timeout = akkaConf.ddata.askTimeout

      ctx.log.info(s"Distributed data actor[$cacheId] started.")
      action().onFailure[Exception](restart)
    }

}
