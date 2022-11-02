package potamoi.conf

import potamoi.common.ComplexEnum
import potamoi.conf.LogsLevel.{toZIOLogLevel, LogsLevel}
import potamoi.conf.LogsStyle.LogsStyle
import potamoi.conf.PotaLogger.Formatter.{logAnnoIfNonEmpty, sourceLoc}
import potamoi.slf4j.Slf4jBridge
import zio.logging.LogFormat._
import zio.logging.{console, consoleJson, LogColor, LogFormat}
import zio.{LogLevel, Trace, ULayer, ZLayer}

import java.time.format.DateTimeFormatter

/**
 * Potamoi ZIO logger layer.
 * Automatic transfer of slf4j logs to zio-logging, and support for automatic extraction
 * of original slf4j MDC, thread name and logger name info to zio-logging annotations.
 */
object PotaLogger {

  /**
   * MDC keys that allowed to be received from non-zio Slf4j pipeline.
   */
  private val defaultAcceptedSlf4jMdc = Set("@loggerName", "@threadName", "akkaSource")
  private val excludeAnnoKeys         = Set("@loggerName", "@threadName")

  /**
   * Standard log format for Potamoi.
   */
  val stLogFormat: LogFormat = {
    label("ts", timestamp(DateTimeFormatter.ISO_LOCAL_DATE_TIME).fixed(26)) |-|
    label("level", level) |-|
    label("fiber", fiberId) |-|
    sourceLoc |-|
    logAnnoIfNonEmpty +
    label("msg", quoted(line)) +
    ifCauseNonEmpty(space + label("cause", cause))
  }

  /**
   * Colored standard log format for Potamoi.
   */
  val stLogFormatColored: LogFormat = {
    label("ts", timestamp(DateTimeFormatter.ISO_LOCAL_DATE_TIME).fixed(26)).color(LogColor.BLUE) |-|
    label("level", level).highlight |-|
    label("fiber", fiberId).color(LogColor.WHITE) |-|
    sourceLoc.color(LogColor.WHITE) |-|
    logAnnoIfNonEmpty.color(LogColor.WHITE) +
    label("msg", quoted(line)).highlight +
    ifCauseNonEmpty(space + label("cause", cause))
  }

  object Formatter {

    /**
     * Source code location or Slf4j logger logging formatter.
     */
    private[potamoi] def sourceLoc: LogFormat =
      LogFormat.make { (builder, trace, _, _, _, _, _, _, annotations) =>
        (annotations.get("@loggerName"), trace) match {
          // from slf4j
          case (Some(loggerName), _) =>
            annotations.get("@threadName") match {
              case None => ()
              case Some(threadName) =>
                builder.appendKeyValue("thread", threadName)
                builder.appendText(" ")
            }
            builder.appendKeyValue("loc", loggerName)
          // from zio system
          case (None, Trace(location, file, line)) =>
            builder.appendKeyValue("loc", location)
            builder.appendText(" ")
            builder.appendKeyValue("file", file)
            builder.appendText(" ")
            builder.appendKeyValue("line", line.toString)
        }
      }

    /**
     * ZIO annotation logging formatter.
     */
    private[potamoi] def logAnnoIfNonEmpty: LogFormat =
      LogFormat.make { (builder, _, _, _, _, _, _, _, annotations) =>
        annotations.foreach { case (key, value) =>
          if (!excludeAnnoKeys.contains(key)) {
            builder.appendKeyValue(key, value)
            builder.appendText(" ")
          }
        }
      }
  }

  /**
   * Generate Potamoi ZIO logger layer.
   *
   * @param level   logging level.
   * @param style   Log line style.
   * @param colored Whether to colorize log line.
   * @param revise  revise log format.
   */
  def logLayer(
      level: LogsLevel = LogsLevel.INFO,
      style: LogsStyle = LogsStyle.Plain,
      colored: Boolean = true,
      allowedMdc: Set[String] = Set.empty,
      revise: LogFormat => LogFormat = identity): ULayer[Unit] = {

    val logFormat = revise(if (colored) stLogFormatColored else stLogFormat)
    val logLevel  = toZIOLogLevel(level)
    val logLayer = style match {
      case LogsStyle.Plain => console(logFormat, logLevel)
      case LogsStyle.Json  => consoleJson(logFormat, logLevel)
    }
    zio.Runtime.removeDefaultLoggers >>> logLayer >+> Slf4jBridge.initialize(logLevel, (defaultAcceptedSlf4jMdc ++ allowedMdc).toVector)
  }

  /**
   * Living ZIO layer.
   */
  val live: ZLayer[PotaConf, Nothing, Unit] = {
    ZLayer.service[PotaConf].project { conf =>
      logLayer(conf.log.level, conf.log.style, conf.log.colored)
    }
  }

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

  def toZIOLogLevel(level: LogsLevel): LogLevel = level match {
    case TRACE   => LogLevel.Trace
    case DEBUG   => LogLevel.Debug
    case INFO    => LogLevel.Info
    case WARNING => LogLevel.Warning
    case ERROR   => LogLevel.Error
    case FATAL   => LogLevel.Fatal
  }
}
