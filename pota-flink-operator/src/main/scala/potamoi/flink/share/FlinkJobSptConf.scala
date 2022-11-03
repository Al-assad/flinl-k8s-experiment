package potamoi.flink.share

import potamoi.common.ComplexEnum
import potamoi.flink.share.SavepointFormatType.SavepointFormatType

/**
 * Flink job savepoint conf.
 */
case class FlinkJobSptConf(enable: Boolean, savepointPath: Option[String] = None, formatType: Option[SavepointFormatType] = None)

object FlinkJobSptConf {
  def disable = FlinkJobSptConf(enable = false)

  def enable(savepointPath: Option[String] = None, formatType: Option[SavepointFormatType] = None): FlinkJobSptConf =
    FlinkJobSptConf(enable = true, savepointPath, formatType)
}

object SavepointFormatType extends ComplexEnum {
  type SavepointFormatType = Value
  val Canonical = Value("CANONICAL")
  val Native    = Value("NATIVE")
}
