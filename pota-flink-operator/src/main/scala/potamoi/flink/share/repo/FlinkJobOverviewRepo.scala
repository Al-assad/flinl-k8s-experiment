package potamoi.flink.share.repo

import io.getquill.{EntityQuery, Insert, Quoted}
import potamoi.db.{QuillCtx, UQIO}
import potamoi.flink.share.model.{Fcid, Fjid, FlinkJobOverview}
import zio.{IO, ZIO}

import java.sql.SQLException

/**
 * Repository for [[FlinkJobOverview]].
 */
case class FlinkJobOverviewRepo(cur: CurFlinkJobOvRepo, hist: HistFlinkJobOvRepo) {
  def collect(ovs: Vector[FlinkJobOverview]): UQIO[Unit] = cur.upsertAll(ovs) *> hist.insertAll(ovs)
}

object FlinkJobOverviewRepo {
  def persist(ctx: QuillCtx): FlinkJobOverviewRepo = FlinkJobOverviewRepo(CurFlinkJobOvPersistRepo(ctx), HistFlinkJobOvPersistRepo(ctx))
}

/**
 * Instantaneous data of [[FlinkJobOverview]].
 */
trait CurFlinkJobOvRepo {
  def upsert(ov: FlinkJobOverview): UQIO[Unit]
  def upsertAll(ovs: Vector[FlinkJobOverview]): UQIO[Unit]
  def removeAll(fjidSeq: Vector[Fjid]): UQIO[Unit]
  def removeInCluster(fcid: Fcid): UQIO[Unit]
  def get(fjid: Fjid): UQIO[Option[FlinkJobOverview]]
  def list(fcid: Fcid): UQIO[List[FlinkJobOverview]]
}

/**
 * Historical data of [[FlinkJobOverview]].
 */
trait HistFlinkJobOvRepo {
  def insert(ov: FlinkJobOverview): UQIO[Unit]
  def insertAll(ovs: Vector[FlinkJobOverview]): UQIO[Unit]
  def removeInCluster(fcid: Fcid): UQIO[Unit]
  def list(fjid: Fjid): UQIO[List[FlinkJobOverview]]
}

sealed abstract class FlinkJobOvPersistRepo(ctx: QuillCtx) {
  import ctx._


  // noinspection DuplicatedCode
  implicit class QuerySyntax(query: Quoted[EntityQuery[FlinkJobOverview]]) {
    def filterFcid(fcid: Fcid) = ???
//      query.filter { ov =>
//      ov.clusterId == lift(fcid.clusterId) &&
//      ov.namespace == lift(fcid.namespace)
//    }
    def filterFjid(fjid: Fjid) = ???
//  query.filter { ov =>
//      ov.clusterId == lift(fjid.clusterId) &&
//      ov.namespace == lift(fjid.namespace) &&
//      ov.jobId == lift(fjid.jobId)
//    }
  }

}

case class CurFlinkJobOvPersistRepo(ctx: QuillCtx) extends FlinkJobOvPersistRepo(ctx) with CurFlinkJobOvRepo {
  import ctx._
  val flkJobOv: Quoted[EntityQuery[FlinkJobOverview]] = quote {
    querySchema[FlinkJobOverview](
      "flink_job_overview_cur",
      _.tasks.total        -> "task_total",
      _.tasks.created      -> "task_created",
      _.tasks.scheduled    -> "task_scheduled",
      _.tasks.deploying    -> "task_deploying",
      _.tasks.running      -> "task_running",
      _.tasks.finished     -> "task_finished",
      _.tasks.canceling    -> "task_canceling",
      _.tasks.canceled     -> "task_canceled",
      _.tasks.failed       -> "task_failed",
      _.tasks.reconciling  -> "task_reconciling",
      _.tasks.initializing -> "task_initializing"
    )
  }

  private val upsertOv: FlinkJobOverview => Insert[FlinkJobOverview] = ???
//    ov =>
//    flkJobOv
//      .insertValue(lift(ov))
//      .onConflictUpdate(_.clusterId, _.namespace, _.jobId)(
//        (t, e) => t.jobName -> e.jobName,
//        (t, e) => t.state -> e.state,
//        (t, e) => t.startTs -> e.startTs,
//        (t, e) => t.endTs -> e.endTs,
//        (t, e) => t.tasks -> e.tasks,
//        (t, e) => t.ts -> e.ts
//      )

  def upsert(ov: FlinkJobOverview): ZIO[Any, SQLException, Unit] = ???
//    run(quote(upsertOv(ov))).unit

  override def upsertAll(ovs: Vector[FlinkJobOverview]): UQIO[Unit] = ???
//    run(quote {
//    liftQuery(ovs).foreach(upsertOv(_))
//  }).unit

  override def get(fjid: Fjid): IO[SQLException, Option[FlinkJobOverview]] = ???
//    run(quote(flkJobOv.filterFjid(fjid))).map(_.headOption)

  override def list(fcid: Fcid): UQIO[List[FlinkJobOverview]] = ???
//    run(quote {
//    flkJobOv.filterFcid(fcid)
//  })

  override def removeAll(fjidSeq: Vector[Fjid]): UQIO[Unit] = ???
//    run(quote {
//    liftQuery(fjidSeq).foreach { fjid =>
//      flkJobOv.filterFjid(fjid).delete
//    }
//  }).unit

  override def removeInCluster(fcid: Fcid): UQIO[Unit] = ???
//    run(quote {
//    flkJobOv.filter(ov => ov.clusterId == fcid.clusterId && ov.namespace == fcid.namespace).delete
//  }).unit
}

case class HistFlinkJobOvPersistRepo(ctx: QuillCtx) extends FlinkJobOvPersistRepo(ctx) with HistFlinkJobOvRepo {
  import ctx._
  val flkJobOv = quote {
    querySchema[FlinkJobOverview](
      "flink_job_overview_hist",
      _.tasks.total        -> "task_total",
      _.tasks.created      -> "task_created",
      _.tasks.scheduled    -> "task_scheduled",
      _.tasks.deploying    -> "task_deploying",
      _.tasks.running      -> "task_running",
      _.tasks.finished     -> "task_finished",
      _.tasks.canceling    -> "task_canceling",
      _.tasks.canceled     -> "task_canceled",
      _.tasks.failed       -> "task_failed",
      _.tasks.reconciling  -> "task_reconciling",
      _.tasks.initializing -> "task_initializing"
    )
  }

  override def insert(ov: FlinkJobOverview): UQIO[Unit] = ???
//    run(quote {
//    flkJobOv.insertValue(lift(ov))
//  }).unit

  override def insertAll(ovs: Vector[FlinkJobOverview]): UQIO[Unit] = ???
//    run(quote {
//    liftQuery(ovs).foreach(flkJobOv.insertValue(_))
//  }).unit

  override def list(fjid: Fjid): UQIO[List[FlinkJobOverview]] = ???
//  run(quote {
//    flkJobOv.filterFjid(fjid).sortBy(_.ts)(desc)
//  })

  override def removeInCluster(fcid: Fcid): UQIO[Unit] = ???
//    run(quote {
//    flkJobOv.filterFcid(fcid).delete
//  }).unit
}
