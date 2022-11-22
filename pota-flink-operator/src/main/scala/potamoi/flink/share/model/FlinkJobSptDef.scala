package potamoi.flink.share.model

import potamoi.common.ComplexEnum
import potamoi.flink.share.model.SptFormatType.SptFormatType

/**
 * Flink job savepoint conf.
 */
case class FlinkJobSptDef(
    drain: Boolean = false,
    savepointPath: Option[String] = None,
    formatType: Option[SptFormatType] = None,
    triggerId: Option[String] = None)

object SptFormatType extends ComplexEnum {
  type SptFormatType = Value
  val Canonical = Value("CANONICAL")
  val Native    = Value("NATIVE")
}
