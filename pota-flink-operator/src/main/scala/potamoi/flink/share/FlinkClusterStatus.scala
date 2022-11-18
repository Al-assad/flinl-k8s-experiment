package potamoi.flink.share

import potamoi.flink.share.FlinkExecMode.FlinkExecMode

/**
 * Flink cluster status overview.
 */
case class FlinkClusterStatus(
    clusterId: String,
    namespace: String,
    flinkVersion: String,
    execMode: FlinkExecMode,
    tmTotal: Int,
    slotsTotal: Int,
    jobsRunning: Int,
    jobsFinished: Int,
    jobsCancelled: Int,
    jobsFailed: Int,
)

case class JmMetric(
    heapMax: Long,
    heapUsed: Long,
    nonHeapMax: Long,
    nonHeapUsed: Long,
    directMemUsed: Long,
    directMemCapacity: Long,
)

case class TmMetric(
    slotsNum: Int,
    slotsFree: Int,
    cpuCores: Int,
    physMem: Long,
    heapMemMax: Long,
    heapMemUsed: Long,
    nonHeapMemMax: Long,
    nonHeapMemUsed: Long,
    directMemUsed: Long,
    mappedMemUsed: Long,
)
