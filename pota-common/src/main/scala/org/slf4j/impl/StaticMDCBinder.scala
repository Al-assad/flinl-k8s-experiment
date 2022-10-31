package org.slf4j.impl

import org.slf4j.helpers.BasicMDCAdapter
import org.slf4j.spi.MDCAdapter

class StaticMDCBinder {
  def getMDCA: MDCAdapter           = StaticMDCBinder.mdcAdapter
  def getMDCAdapterClassStr: String = StaticMDCBinder.mdcAdapterClassStr
}

object StaticMDCBinder {
  val SINGLETON: StaticMDCBinder = new StaticMDCBinder
  val mdcAdapter                 = new BasicMDCAdapter
  val mdcAdapterClassStr         = classOf[BasicMDCAdapter].getName
}
