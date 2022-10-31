package org.slf4j.impl

import org.slf4j.ILoggerFactory
import org.slf4j.spi.LoggerFactoryBinder

//noinspection ConvertNullInitializerToUnderscore
class StaticLoggerBinder extends LoggerFactoryBinder {
  override def getLoggerFactory: ILoggerFactory = StaticLoggerBinder.loggerFactory
  override def getLoggerFactoryClassStr: String = StaticLoggerBinder.loggerFactoryClassStr
}

//noinspection AccessorLikeMethodIsEmptyParen
object StaticLoggerBinder {
  val SINGLETON: StaticLoggerBinder = new StaticLoggerBinder()
  val REQUESTED_API_VERSION: String = "1.6.99"

  val loggerFactory                 = new ZIOLoggerFactory
  val loggerFactoryClassStr: String = classOf[ZIOLoggerFactory].getName

  def getSingleton(): StaticLoggerBinder = StaticLoggerBinder.SINGLETON
}
