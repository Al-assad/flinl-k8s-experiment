package potamoi.flink.share

import potamoi.common.ComplexEnum
import potamoi.flink.share.SptFormatType.SptFormatType

/**
 * Flink job savepoint conf.
 */
case class FlinkJobSptConf(enable: Boolean, savepointPath: Option[String] = None, formatType: Option[SptFormatType] = None)

object FlinkJobSptConf {
  def disable = FlinkJobSptConf(enable = false)

  def enable(savepointPath: Option[String] = None, formatType: Option[SptFormatType] = None): FlinkJobSptConf =
    FlinkJobSptConf(enable = true, savepointPath, formatType)
}

object SptFormatType extends ComplexEnum {
  type SptFormatType = Value
  val Canonical = Value("CANONICAL")
  val Native    = Value("NATIVE")
}
