package potamoi.flink.share

import potamoi.flink.share.FlinkExecMode.FlinkExecMode

/**
 * Flink cluster status overview.
 */
case class FlinkClusterInfo(
    clusterId: String,
    namespace: String,
    flinkVersion: String,
    execMode: FlinkExecMode,
    tmTotal: Int,
    slotsTotal: Int,
    jobs: JobsStats
)

case class JobsStats(running: Int, finished: Int, canceled: Int, failed: Int)

case class JmMetric(
    heapMax: Long,
    heapUsed: Long,
)

case class TmMetric(
    slotsTotal: Int,
    slotsFree: Int,
    cpuCores: Int,
    physMem: Long,
    heapMemMax: Long,
    heapMemUsed: Long,
)

case class FlinkClusterK8sResourceRef()

case class K8sService(name: String)
