package org.slf4j.impl

import org.slf4j.IMarkerFactory
import org.slf4j.helpers.BasicMarkerFactory
import org.slf4j.spi.MarkerFactoryBinder

class StaticMarkerBinder extends MarkerFactoryBinder {
  override def getMarkerFactory: IMarkerFactory = StaticMarkerBinder.SINGLETON
  override def getMarkerFactoryClassStr: String = StaticMarkerBinder.markFactoryClassStr
}

object StaticMarkerBinder {
  val SINGLETON           = new BasicMarkerFactory
  val markFactoryClassStr = classOf[BasicMarkerFactory].getName
}
