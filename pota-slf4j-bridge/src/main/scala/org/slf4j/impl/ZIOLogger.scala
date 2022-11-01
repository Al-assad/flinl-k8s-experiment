package org.slf4j.impl

import org.slf4j.MDC
import org.slf4j.helpers.{MarkerIgnoringBase, MessageFormatter}
import zio.{Cause, UIO, ZIO, ZIOAspect}

import scala.collection.JavaConverters._

class ZIOLogger(name: String, factory: ZIOLoggerFactory) extends MarkerIgnoringBase {

  def this(name: String) {
    this(name, null)
  }

  private val mdcKeys         = factory.getFilterMdcKeys
  private val shouldHandleMdc = mdcKeys.nonEmpty

  private def run(f: ZIO[Any, Nothing, Unit]): Unit = {
    factory.run(ZIO.logSpan(name) {
      if (shouldHandleMdc) wrapMdcLogAnnoAspect(f) else f
    })
  }

  private def wrapMdcLogAnnoAspect(f: UIO[Unit]) = {
    Option(MDC.getCopyOfContextMap).map(_.asScala.filter(kv => mdcKeys.contains(kv._1))) match {
      case None         => f
      case Some(mdcMap) => f @@ mdcMap.map(kv => ZIOAspect.annotated(kv._1, kv._2)).reduce(_ @@ _)
    }
  }

  override def isTraceEnabled: Boolean = true

  override def isDebugEnabled: Boolean = true

  override def isInfoEnabled: Boolean = true

  override def isWarnEnabled: Boolean = true

  override def isErrorEnabled: Boolean = true

  override def trace(msg: String): Unit = run {
    ZIO.logTrace(msg)
  }

  override def trace(format: String, arg: AnyRef): Unit = run {
    ZIO.logTrace(MessageFormatter.format(format, arg).getMessage)
  }

  override def trace(format: String, arg1: AnyRef, arg2: AnyRef): Unit = run {
    ZIO.logTrace(MessageFormatter.format(format, arg1, arg2).getMessage)
  }

  override def trace(format: String, arguments: AnyRef*): Unit = run {
    ZIO.logTrace(MessageFormatter.arrayFormat(format, arguments.toArray).getMessage)
  }

  override def trace(msg: String, t: Throwable): Unit = run {
    ZIO.logTraceCause(msg, Option(t).map(t => Cause.die(t)).getOrElse(Cause.empty))
  }

  override def debug(msg: String): Unit = run {
    ZIO.logDebug(msg)
  }

  override def debug(format: String, arg: AnyRef): Unit = run {
    ZIO.logDebug(MessageFormatter.format(format, arg).getMessage)
  }

  override def debug(format: String, arg1: AnyRef, arg2: AnyRef): Unit = run {
    ZIO.logDebug(MessageFormatter.format(format, arg1, arg2).getMessage)
  }

  override def debug(msg: String, t: Throwable): Unit = run {
    ZIO.logDebugCause(msg, Option(t).map(t => Cause.die(t)).getOrElse(Cause.empty))
  }

  override def debug(format: String, arguments: AnyRef*): Unit = run {
    ZIO.logDebug(MessageFormatter.arrayFormat(format, arguments.toArray).getMessage)
  }

  override def info(msg: String): Unit = run {
    ZIO.logInfo(msg)
  }

  override def info(format: String, arg: AnyRef): Unit = run {
    ZIO.logInfo(MessageFormatter.format(format, arg).getMessage)
  }

  override def info(format: String, arg1: AnyRef, arg2: AnyRef): Unit = run {
    ZIO.logInfo(MessageFormatter.format(format, arg1, arg2).getMessage)
  }

  override def info(format: String, arguments: AnyRef*): Unit = run {
    ZIO.logInfo(MessageFormatter.arrayFormat(format, arguments.toArray).getMessage)
  }

  override def info(msg: String, t: Throwable): Unit = run {
    ZIO.logInfoCause(msg, Option(t).map(t => Cause.die(t)).getOrElse(Cause.empty))
  }

  override def warn(msg: String): Unit = run {
    ZIO.logWarning(msg)
  }

  override def warn(format: String, arg: AnyRef): Unit = run {
    ZIO.logWarning(MessageFormatter.format(format, arg).getMessage)
  }

  override def warn(format: String, arg1: AnyRef, arg2: AnyRef): Unit = run {
    ZIO.logWarning(MessageFormatter.format(format, arg1, arg2).getMessage)
  }

  override def warn(format: String, arguments: AnyRef*): Unit = run {
    ZIO.logWarning(MessageFormatter.arrayFormat(format, arguments.toArray).getMessage)
  }

  override def warn(msg: String, t: Throwable): Unit = run {
    ZIO.logWarningCause(msg, Option(t).map(t => Cause.die(t)).getOrElse(Cause.empty))
  }

  override def error(msg: String): Unit = run {
    ZIO.logError(msg)
  }

  override def error(format: String, arg: AnyRef): Unit = run {
    ZIO.logError(MessageFormatter.format(format, arg).getMessage)
  }

  override def error(format: String, arg1: AnyRef, arg2: AnyRef): Unit = run {
    ZIO.logError(MessageFormatter.format(format, arg1, arg2).getMessage)
  }

  override def error(format: String, arguments: AnyRef*): Unit = run {
    ZIO.logError(MessageFormatter.arrayFormat(format, arguments.toArray).getMessage)
  }

  override def error(msg: String, t: Throwable): Unit = run {
    ZIO.logErrorCause(msg, Option(t).map(t => Cause.die(t)).getOrElse(Cause.empty))
  }

}
