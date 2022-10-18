package kce.conf

object KceConf {
  val default: KceConf = KceConf(localStoDir = "kce")
}

case class KceConf(localStoDir: String)
