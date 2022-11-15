package potamoi.conf

import com.softwaremill.quicklens.ModifyPimp
import potamoi.logger.LogsLevel.LogsLevel
import potamoi.logger.{LogsLevel, LogsStyle}
import potamoi.logger.LogsStyle.LogsStyle
import zio.config.magnolia.name
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Logging configuration.
 */
case class LogConf(
    level: LogsLevel = LogsLevel.INFO,
    style: LogsStyle = LogsStyle.Plain,
    colored: Boolean = true,
    @name("in-one-line") inOneLine: Boolean = false)
    extends Resolvable {

  override def resolve = { root =>
    root
      .modify(_.log)
      .using { conf =>
        if (conf.style == LogsStyle.Json) conf.copy(colored = false, inOneLine = true) else conf
      }
  }
}

object LogConf {
  implicit val codec: JsonCodec[LogConf] = DeriveJsonCodec.gen[LogConf]
}
