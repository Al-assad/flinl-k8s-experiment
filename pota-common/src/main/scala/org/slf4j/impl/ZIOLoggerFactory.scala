package org.slf4j.impl

import org.slf4j.{ILoggerFactory, Logger}
import zio.{Unsafe, ZIO}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

//noinspection ConvertNullInitializerToUnderscore
class ZIOLoggerFactory extends ILoggerFactory {

  private var runtime: zio.Runtime[Any] = null
  private val loggers                   = new ConcurrentHashMap[String, Logger]().asScala

  def attachRuntime(runtime: zio.Runtime[Any]): Unit = this.runtime = runtime

  private[slf4j] def run(f: ZIO[Any, Nothing, Any]): Unit = {
    if (runtime != null) Unsafe.unsafe { implicit u => runtime.unsafe.run(f) }
  }

  override def getLogger(name: String): Logger = loggers.getOrElseUpdate(name, new ZIOLogger(name, this))
}

object ZIOLoggerFactory {
  def initialize(runtime: zio.Runtime[Any]): Unit =
    StaticLoggerBinder.SINGLETON.getLoggerFactory.asInstanceOf[ZIOLoggerFactory].attachRuntime(runtime)
}
