package kce.flink.operator.entity

import kce.common.GenericPF
import kce.common.ScalaVer.{Scala212, ScalaVer}
import kce.flink.operator.entity.FlinkVer.extractMajorVer
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink version.
 */
case class FlinkVer(ver: String, scalaVer: ScalaVer = Scala212) {
  lazy val majorVer = extractMajorVer(ver)
}

object FlinkVer {
  implicit val codec: JsonCodec[FlinkVer] = DeriveJsonCodec.gen[FlinkVer]

  def extractMajorVer(flinkVer: String) = {
    flinkVer.split('.').contra { part =>
      if (part.length < 2) flinkVer
      else part(0) + "." + part(1)
    }
  }
}


object test extends App {
  println("Hello")
}