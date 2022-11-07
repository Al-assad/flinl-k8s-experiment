package potamoi.flink.share

import potamoi.common.ComplexEnum

/**
 * Flink job status.
 * see: [[org.apache.flink.api.common.JobStatus]]
 */
object FlinkJobStatus extends ComplexEnum {
  type FlinkJobStatus = Value

  val INITIALIZING, CREATED, RUNNING, FAILING, FAILED, CANCELLING, CANCELED, FINISHED, RESTARTING, SUSPENDED, RECONCILING = Value
}
