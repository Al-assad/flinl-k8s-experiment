package potamoi.flink.share.model

import potamoi.common.ComplexEnum
import potamoi.curTs
import potamoi.flink.share.model.JobState.JobState
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink job overview.
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
  lazy val fjid: Fjid  = Fjid(clusterId, namespace, jobId)
  def durationTs: Long = curTs - startTs
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
  implicit val tasksCodec: JsonCodec[TaskStats]    = DeriveJsonCodec.gen[TaskStats]
  implicit val codec: JsonCodec[FlinkJobOverview]  = DeriveJsonCodec.gen[FlinkJobOverview]
  implicit val sorting: Ordering[FlinkJobOverview] = Ordering.by(e => (e.clusterId, e.namespace, e.jobId))
}

/**
 * Flink job state.
 * see: [[org.apache.flink.api.common.JobStatus]]
 */
object JobState extends ComplexEnum {
  type JobState = Value

  val INITIALIZING, CREATED, RUNNING, FAILING, FAILED, CANCELLING, CANCELED, FINISHED, RESTARTING, SUSPENDED, RECONCILING = Value
}
