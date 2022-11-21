package potamoi.flink.share

import potamoi.common.ComplexEnum
import potamoi.curTs
import potamoi.flink.share.JobState.JobState
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink job status overview.
 */
case class FlinkJobOverview(
    clusterId: String,
    namespace: String,
    jobId: String,
    jobName: String,
    state: JobState,
    startTs: Long,
    endTs: Long,
    tasks: TaskStats,
    ts: Long) {
  def durationTs: Long = curTs - startTs
  def fjid: Fjid       = Fjid(clusterId, namespace, jobId)
}

case class TaskStats(
    total: Int,
    created: Int,
    scheduled: Int,
    deploying: Int,
    running: Int,
    finished: Int,
    canceling: Int,
    canceled: Int,
    failed: Int,
    reconciling: Int,
    initializing: Int)

object FlinkJobOverview {
  implicit def tasksCodec: JsonCodec[TaskStats]   = DeriveJsonCodec.gen[TaskStats]
  implicit def codec: JsonCodec[FlinkJobOverview] = DeriveJsonCodec.gen[FlinkJobOverview]
}

/**
 * Flink job state.
 * see: [[org.apache.flink.api.common.JobStatus]]
 */
object JobState extends ComplexEnum {
  type JobState = Value

  val INITIALIZING, CREATED, RUNNING, FAILING, FAILED, CANCELLING, CANCELED, FINISHED, RESTARTING, SUSPENDED, RECONCILING = Value
}
