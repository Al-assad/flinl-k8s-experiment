package potamoi.flink.share

import potamoi.common.{curTs, ComplexEnum}
import potamoi.flink.operator.FlinkRestRequest.TaskStats
import potamoi.flink.share.JobState.JobState
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink job status overview.
 */
case class FlinkJobStatus(jobId: String, name: String, state: JobState, startTs: Long, endTs: Long, tasks: TaskStats) {
  def durationTs: Long = curTs - startTs
}

object FlinkJobStatus {
  implicit def codec: JsonCodec[FlinkJobStatus]     = DeriveJsonCodec.gen[FlinkJobStatus]
  implicit def tasksCodec: JsonCodec[TaskStats] = DeriveJsonCodec.gen[TaskStats]
}

/**
 * Flink job state.
 * see: [[org.apache.flink.api.common.JobStatus]]
 */
object JobState extends ComplexEnum {
  type JobState = Value

  val INITIALIZING, CREATED, RUNNING, FAILING, FAILED, CANCELLING, CANCELED, FINISHED, RESTARTING, SUSPENDED, RECONCILING = Value
}
