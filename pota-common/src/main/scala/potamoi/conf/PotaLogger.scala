package potamoi.conf

import potamoi.common.ComplexEnum
import potamoi.conf.LogsLevel.LogsLevel
import potamoi.conf.LogsStyle.LogsStyle
import potamoi.slf4j.Slf4jBridge
import zio.logging.LogFormat._
import zio.logging.{console, consoleJson, LogColor, LogFormat}
import zio.{LogLevel, ULayer, ZLayer}

import java.time.format.DateTimeFormatter

/**
 * Potamoi ZIO logger layer.
 */
object PotaLogger {

  /**
   * MDC keys that allowed to be received from non-zio Slf4j pipeline.
   */
  private val acceptedSlf4jMdc = Vector("akkaSource")

  /**
   * Standard log format for Potamoi.
   */
  val stLogFormat: LogFormat = {
    label("ts", timestamp(DateTimeFormatter.ISO_LOCAL_DATE_TIME).fixed(26)) |-|
    label("level", level) |-|
    label("thread", fiberId) |-|
    allAnnotations |-|
    label("message", quoted(line)) +
    ifCauseNonEmpty(space + label("cause", cause))
  }

  /**
   * Colored standard log format for Potamoi.
   */
  val stLogFormatColored: LogFormat = {
    label("ts", timestamp(DateTimeFormatter.ISO_LOCAL_DATE_TIME).fixed(26)).color(LogColor.BLUE) |-|
    label("level", level).highlight |-|
    label("thread", fiberId).color(LogColor.WHITE) |-|
    allAnnotations.color(LogColor.WHITE) |-|
    label("message", quoted(line)).highlight +
    ifCauseNonEmpty(space + label("cause", cause))
  }

  /**
   * Generate Potamoi ZIO logger layer.
   *
   * @param level logging level.
   * @param style Log line style.
   * @param colored Whether to colorize log line.
   * @param revise revise log format.
   */
  def logLayer(
      level: LogsLevel = LogsLevel.INFO,
      style: LogsStyle = LogsStyle.Plain,
      colored: Boolean = true,
      revise: LogFormat => LogFormat = identity): ULayer[Unit] = {

    val logFormat = revise(if (colored) stLogFormatColored else stLogFormat)
    val logLevel = level match {
      case LogsLevel.TRACE   => LogLevel.Trace
      case LogsLevel.DEBUG   => LogLevel.Debug
      case LogsLevel.INFO    => LogLevel.Info
      case LogsLevel.WARNING => LogLevel.Warning
      case LogsLevel.ERROR   => LogLevel.Error
      case LogsLevel.FATAL   => LogLevel.Fatal
    }
    val logLayer = style match {
      case LogsStyle.Plain => console(logFormat, logLevel)
      case LogsStyle.Json  => consoleJson(logFormat, logLevel)
    }
    zio.Runtime.removeDefaultLoggers >>> logLayer >+> Slf4jBridge.initialize(acceptedSlf4jMdc)
  }

  /**
   * Living ZIO layer.
   */
  val live: ZLayer[PotaConf, Nothing, Unit] = {
    ZLayer.service[PotaConf].project { conf =>
      logLayer(conf.log.level, conf.log.style, conf.log.colored)
    }
  }

  /**
   * Default ZIO layer.
   */
  val default: ULayer[Unit] = logLayer()

  /**
   * Debugging ZIO layer.
   */
  val debug: ULayer[Unit] = logLayer(level = LogsLevel.DEBUG)

}

/**
 * Potamoi logging line style.
 */
object LogsStyle extends ComplexEnum {
  type LogsStyle = Value
  val Plain = Value("plain")
  val Json  = Value("json")
}

/**
 * Logging level.
 */
object LogsLevel extends ComplexEnum {
  type LogsLevel = Value
  val TRACE, DEBUG, INFO, WARNING, ERROR, FATAL = Value
}
