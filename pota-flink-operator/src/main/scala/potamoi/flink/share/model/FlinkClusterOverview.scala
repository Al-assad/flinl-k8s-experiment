package potamoi.flink.share.model

import potamoi.flink.share.model.FlinkExecMode.FlinkExecMode
import zio.json.DeriveJsonCodec

/**
 * Flink cluster overview.
 */
case class FlinkClusterOverview(
    clusterId: String,
    namespace: String,
    execMode: FlinkExecMode,
    tmTotal: Int,
    slotsTotal: Int,
    jobs: JobsStats) {
  lazy val fcid = Fcid(clusterId, namespace)
}

case class JobsStats(
    running: Int,
    finished: Int,
    canceled: Int,
    failed: Int)

object FlinkClusterOverview {
  implicit val jobStatsCodec = DeriveJsonCodec.gen[JobsStats]
  implicit val codec         = DeriveJsonCodec.gen[FlinkClusterOverview]
}

//case class JmMetric(
//    heapMax: Long,
//    heapUsed: Long)
//
//case class TmMetric(
//    slotsTotal: Int,
//    slotsFree: Int,
//    cpuCores: Int,
//    physMem: Long,
//    heapMemMax: Long,
//    heapMemUsed: Long)
