package potamoi.flink.share.model

import potamoi.common.ComplexEnum
import potamoi.flink.share.model.OprState.OprState
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink savepoint trigger status.
 */
case class FlinkSptTriggerStatus(state: OprState, failureCause: Option[String]) {
  lazy val isCompleted = state == OprState.Completed
  lazy val isFailed    = failureCause.isDefined
}

object FlinkSptTriggerStatus {
  implicit def codec: JsonCodec[FlinkSptTriggerStatus] = DeriveJsonCodec.gen[FlinkSptTriggerStatus]
}

object OprState extends ComplexEnum {
  type OprState = Value
  val Completed  = Value("COMPLETED")
  val InProgress = Value("IN_PROGRESS")
}
