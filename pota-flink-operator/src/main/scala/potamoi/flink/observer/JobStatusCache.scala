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
import potamoi.flink.share.{Fcid, Fjid, FlinkJobStatus, JobId}

/**
 * Flink job overview info distributed data storage base on LWWMap.
 */
private[observer] object JobStatusCache extends LWWMapDData[Fjid, FlinkJobStatus] {

  val cacheId    = "flink-job-status"
  val writeLevel = WriteLocal
  val readLevel  = ReadLocal

  final case class ListJobIdUnderFcid(fcid: Fcid, reply: ActorRef[Vector[JobId]])           extends GetCmd
  final case class ListRecordUnderFcid(fcid: Fcid, reply: ActorRef[Vector[FlinkJobStatus]]) extends GetCmd
  final case class SelectRecord(
      filter: (Fjid, FlinkJobStatus) => Boolean,
      drop: Option[Int] = None,
      take: Option[Int] = None,
      reply: ActorRef[Vector[FlinkJobStatus]])
      extends GetCmd

  final case class RemoveRecordUnderFcid(fcid: Fcid) extends UpdateCmd

  def apply(akkaConf: AkkaConf): Behavior[Cmd] =
    Behaviors.setup { implicit ctx =>
      implicit val node             = DistributedData(ctx.system).selfUniqueAddress
      implicit val timeout: Timeout = akkaConf.defaultAskTimeout
      ctx.log.info(s"Distributed data actor[$cacheId] started.")

      action(
        get = { (cmd, cache) =>
          cmd match {
            case ListJobIdUnderFcid(fcid, reply)  => reply ! cache.entries.keys.filter(_.isUnder(fcid)).map(_.jobId).toVector
            case ListRecordUnderFcid(fcid, reply) => reply ! cache.entries.filter(kv => kv._1.isUnder(fcid)).values.toVector
            case SelectRecord(filter, drop, take, reply) =>
              val query = {
                val q  = cache.entries.view.filter(kv => filter(kv._1, kv._2))
                val q2 = if (drop.isDefined) q.drop(drop.get) else q
                val q3 = if (take.isDefined) q2.take(take.get) else q2
                q3
              }
              reply ! query.map(_._2).toVector
          }
        },
        notYetInit = {
          case ListJobIdUnderFcid(_, reply)  => reply ! Vector.empty
          case ListRecordUnderFcid(_, reply) => reply ! Vector.empty
          case SelectRecord(_, _, _, reply)  => reply ! Vector.empty
        },
        update = { case (RemoveRecordUnderFcid(fcid), cache) =>
          cache.entries.keys.filter(_.isUnder(fcid)).foreach(cache.remove(node, _))
          cache
        }
      ).onFailure[Exception](restart)
    }

}

object test extends App {
  val seq = Seq(1, 2, 3, 4, 5)
  println(seq.drop(0).take(100))
}
