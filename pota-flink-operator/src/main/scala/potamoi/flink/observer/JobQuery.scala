package potamoi.flink.observer

import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import cats.implicits.{catsSyntaxEitherId, toFoldableOps}
import potamoi.cluster.{CborSerializable, LWWMapDData}
import potamoi.cluster.PotaActorSystem.{ActorGuardian, ActorGuardianExtension}
import potamoi.common.Order.Order
import potamoi.common.{ComplexEnum, PageReq, PageRsp, TsRange}
import potamoi.config.{DDataConf, PotaConf}
import potamoi.flink.observer.JobMetricTracker.{GetJobMetrics, ListJobMetrics}
import potamoi.flink.observer.JobQryTerm.SortField.SortField
import potamoi.flink.observer.JobQryTerm.{Filter, SortField, SortTerm}
import potamoi.flink.observer.JobOvTracker.{GetJobOverview, GetJobOverviews, ListJobOverviews}
import potamoi.flink.share.model.JobState.JobState
import potamoi.flink.share.model.{Fcid, Fjid, FlinkJobMetrics, FlinkJobOverview}
import potamoi.flink.share.{FlinkIO, FlinkOprErr, JobId}
import potamoi.timex._
import zio.stream.ZStream

/**
 * Flink jobs snapshot information query layer.
 */
trait JobQuery {
  def getOverview(fjid: Fjid): FlinkIO[Option[FlinkJobOverview]]
  def listOverview(fcid: Fcid): FlinkIO[List[FlinkJobOverview]]
  def listAllOverview: FlinkIO[List[FlinkJobOverview]]

  def listJobId(fcid: Fcid): FlinkIO[Set[JobId]]
  def listAllJobId: FlinkIO[Set[Fjid]]

  def getJobState(fjid: Fjid): FlinkIO[Option[JobState]]
  def listJobState(fcid: Fcid): FlinkIO[Map[JobId, JobState]]

  def selectOverview(filter: Filter, orders: Vector[SortTerm] = Vector.empty): FlinkIO[List[FlinkJobOverview]]
  def pageSelectOverview(filter: Filter, pageReq: PageReq, orders: Vector[SortTerm] = Vector.empty): FlinkIO[PageRsp[FlinkJobOverview]]

  def getMetrics(fjid: Fjid): FlinkIO[Option[FlinkJobMetrics]]
  def listMetrics(fcid: Fcid): FlinkIO[List[FlinkJobMetrics]]
}

object JobQryTerm {
  case class Filter(
      fcidIn: Set[Fcid] = Set.empty,
      jobIdIn: Set[JobId] = Set.empty,
      jobNameContains: Option[String] = None,
      jobStateIn: Set[JobState] = Set.empty,
      startTsRange: TsRange = TsRange())

  object SortField extends ComplexEnum {
    type SortField = Value
    val jobName, jobState, startTs = Value
  }

  type SortTerm = (SortField, Order)
}

object JobQuery {

  def live(potaConf: PotaConf, guardian: ActorGuardian, endpointQuery: RestEndpointQuery) =
    for {
      idxCache           <- guardian.spawn(JobIdxCache(potaConf.akka.ddata.getFlinkJobIndex), "flkJobOvIndexCache")
      ovTrackerProxy     <- guardian.spawn(JobOvTrackerProxy(potaConf, endpointQuery), "flkJobOvTrackerProxy")
      metricTrackerProxy <- guardian.spawn(JobMetricTrackerProxy(potaConf, endpointQuery), "flkJobMetricsTrackerProxy")
      queryTimeout     = potaConf.flink.snapshotQuery.askTimeout
      queryParallelism = potaConf.flink.snapshotQuery.parallelism
      sc               = guardian.scheduler
    } yield Live(ovTrackerProxy, metricTrackerProxy, idxCache, queryParallelism)(sc, queryTimeout)

  /**
   * Akka Sharding/DData hybrid storage implementation.
   */
  case class Live(
      ovTrackers: ActorRef[JobOvTrackerProxy.Cmd],
      metricsTrackers: ActorRef[JobMetricTrackerProxy.Cmd],
      idxCache: ActorRef[JobIdxCache.Cmd],
      queryParallelism: Int
    )(implicit sc: Scheduler,
      queryTimeout: Timeout)
      extends JobQuery {

    def getOverview(fjid: Fjid): FlinkIO[Option[FlinkJobOverview]] = ovTrackers(fjid.fcid).ask(GetJobOverview(fjid.jobId, _))
    def listOverview(fcid: Fcid): FlinkIO[List[FlinkJobOverview]]  = ovTrackers(fcid).ask(ListJobOverviews).map(_.toList.sortBy(_.jobId))

    def listJobId(fcid: Fcid): FlinkIO[Set[JobId]]         = idxCache.listKeys.map(_.map(_.jobId))
    def listAllJobId: FlinkIO[Set[Fjid]]                   = idxCache.listKeys
    def getJobState(fjid: Fjid): FlinkIO[Option[JobState]] = idxCache.get(fjid).map(_.map(_.jobState))

    def getMetrics(fjid: Fjid): FlinkIO[Option[FlinkJobMetrics]] = metricsTrackers(fjid.fcid).ask(GetJobMetrics(fjid.jobId, _))
    def listMetrics(fcid: Fcid): FlinkIO[List[FlinkJobMetrics]]  = metricsTrackers(fcid).ask(ListJobMetrics).map(_.toList.sortBy(_.jobId))

    def listJobState(fcid: Fcid): FlinkIO[Map[JobId, JobState]] =
      idxCache.listAll.map {
        _.filter { case (k, _) => k.isUnder(fcid) }
          .map { case (k, v) => k.jobId -> v.jobState }
      }

    def listAllOverview: FlinkIO[List[FlinkJobOverview]] = {
      val listAllFcid: FlinkIO[Set[Fcid]] = idxCache.listKeys.map(_.map(_.fcid).toSet)
      ZStream
        .fromIterableZIO[Any, FlinkOprErr, Fcid](listAllFcid)
        .mapZIOParUnordered(queryParallelism)(listOverview)
        .runFold(Vector.empty[FlinkJobOverview])(_ ++ _)
        .map(_.toList.sorted)
    }

    def selectOverview(filter: Filter, orders: Vector[SortTerm] = Vector.empty): FlinkIO[List[FlinkJobOverview]] = {
      ZStream
        .fromIterableZIO(hitIdxFcid(filter))
        .mapZIOParUnordered(queryParallelism) { case (fcid, jobIds) => ovTrackers(fcid).ask(GetJobOverviews(jobIds, _)) }
        .runFold(Vector.empty[FlinkJobOverview])(_ ++ _)
        .map(sortJobOv(_, orders))
    }

    def pageSelectOverview(filter: Filter, pageReq: PageReq, orders: Vector[SortTerm] = Vector.empty): FlinkIO[PageRsp[FlinkJobOverview]] = {
      val countEle = hitIdxFcid(filter).map(_.size)
      val queryOv =
        ZStream
          .fromIterableZIO(hitIdxFcid(filter, orders, Some(pageReq)))
          .mapZIOParUnordered(queryParallelism) { case (fcid, jobIds) => ovTrackers(fcid).ask(GetJobOverviews(jobIds, _)) }
          .runFold(Vector.empty[FlinkJobOverview])(_ ++ _)

      (countEle <&> queryOv).map { case (totalEle, rs) =>
        PageRsp[FlinkJobOverview](pageReq, totalEle, sortJobOv(rs, orders))
      }
    }

    /**
     * Filter target fcid in JobOvIndexCache.
     */
    private def hitIdxFcid(
        f: Filter,
        orders: Vector[SortTerm] = Vector.empty,
        pageReq: Option[PageReq] = None): FlinkIO[Vector[(Fcid, Set[JobId])]] = idxCache.listAll.map { items =>
      var idx = items.toVector
      // filter clause
      if (f.fcidIn.nonEmpty) idx = idx.filter(e => f.fcidIn.contains(e._1.fcid))
      if (f.jobIdIn.nonEmpty) idx = idx.filter(e => f.jobIdIn.contains(e._1.jobId))
      if (f.jobStateIn.nonEmpty) idx = idx.filter(e => f.jobStateIn.contains(e._2.jobState))
      if (f.jobNameContains.isDefined) idx = idx.filter(e => e._2.jobName.contains(f.jobNameContains.get))
      if (f.startTsRange.isLimited) idx = idx.filter(e => f.startTsRange.judge(e._2.startTs))
      // order clause
      if (orders.nonEmpty) idx = idx.sortWith { (a, b) =>
        orders.foldM(0) { case (_, (field, order)) =>
          val r = field match {
            case SortField.jobName  => a._2.jobName.compare(b._2.jobName) * order.id
            case SortField.jobState => a._2.jobState.compare(b._2.jobState) * order.id
            case SortField.startTs  => a._2.startTs.compare(b._2.startTs) * order.id
          }
          if (r == 0) r.asRight else r.asLeft
        } match {
          case Left(r)  => r < 0
          case Right(r) => r < 0
        }
      }
      // pageable cut
      if (pageReq.isDefined) idx = idx.slice(pageReq.get.offsetRowsInt, pageReq.get.offsetRowsInt + pageReq.get.pagSize)
      // group by fcid
      idx
        .groupBy(_._1.fcid)
        .map(kv => kv._1 -> kv._2.map(_._1.jobId).toSet)
        .toVector
    }

    /**
     * Sort FlinkJobOverview by order roles.
     */
    private def sortJobOv(items: Vector[FlinkJobOverview], orders: Vector[(SortField, Order)]): List[FlinkJobOverview] =
      if (orders.isEmpty) items.sorted.toList
      else {
        items.sortWith { (a, b) =>
          orders.foldM(0) { case (_, (field, order)) =>
            val r = field match {
              case SortField.jobName  => a.jobName.compare(b.jobName) * order.id
              case SortField.jobState => a.state.compare(b.state) * order.id
              case SortField.startTs  => a.startTs.compare(b.startTs) * order.id
            }
            if (r == 0) r.asRight else r.asLeft
          } match {
            case Left(r)  => r < 0
            case Right(r) => r < 0
          }
        }.toList
      }

  }
}

/**
 * Job overview query index cache.
 */
private[observer] object JobIdxCache extends LWWMapDData[Fjid, JobIndex] {
  val cacheId                               = "flink-job-ov-index"
  def apply(conf: DDataConf): Behavior[Cmd] = start(conf)
}

case class JobIndex(jobName: String, jobState: JobState, startTs: Long) extends CborSerializable
object JobIndex {
  def of(ov: FlinkJobOverview): (Fjid, JobIndex) = ov.fjid -> JobIndex(ov.jobName, ov.state, ov.startTs)
}
