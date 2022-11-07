package potamoi.flink.observer

import akka.actor.typed.SupervisorStrategy.restart
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.cluster.ddata.typed.scaladsl.Replicator.{ReadLocal, WriteLocal}
import akka.util.Timeout
import potamoi.cluster.LWWMapDData
import potamoi.common.ActorExtension.BehaviorWrapper
import potamoi.conf.AkkaConf
import potamoi.flink.operator.FlinkRestRequest.JobOverviewInfo
import potamoi.flink.share.{Fcid, Fjid, JobId}

/**
 * Flink job overview info distributed data storage base on LWWMap.
 */
private[observer] object JobOverviewCache extends LWWMapDData[Fjid, JobOverviewInfo] {

  val cacheId    = "flink-job-overview"
  val writeLevel = WriteLocal
  val readLevel  = ReadLocal

  final case class ListJobIdUnderFcid(fcid: Fcid, reply: ActorRef[Set[JobId]]) extends GetCmd
  final case class RemoveRecordUnderFcid(fcid: Fcid)                           extends UpdateCmd

  def apply(akkaConf: AkkaConf): Behavior[Cmd] =
    Behaviors.setup { implicit ctx =>
      implicit val node             = DistributedData(ctx.system).selfUniqueAddress
      implicit val timeout: Timeout = akkaConf.defaultAskTimeout
      ctx.log.info(s"Distributed data actor[$cacheId] started.")

      action(
        get = { case (ListJobIdUnderFcid(fcid, reply), cache) =>
          reply ! cache.entries.keys.filter(_.isUnder(fcid)).map(_.jobId).toSet
        },
        notYetInit = { case ListJobIdUnderFcid(_, reply) =>
          reply ! Set.empty
        },
        update = { case (RemoveRecordUnderFcid(fcid), cache) =>
          cache.entries.keys.filter(_.isUnder(fcid)).foreach(cache.remove(node, _))
          cache
        }
      ).onFailure[Exception](restart)
    }

}
