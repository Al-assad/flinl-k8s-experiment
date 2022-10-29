package potamoi.flink.share

import potamoi.common.GenericPF
import potamoi.common.ScalaVer.{Scala212, ScalaVer}
import potamoi.flink.share.FlinkVer.extractMajorVer
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink version.
 */
case class FlinkVer(ver: String, scalaVer: ScalaVer = Scala212) {
  def majorVer: String = extractMajorVer(ver)
  def fullVer: String  = s"${ver}-scala_${scalaVer}"
}

object FlinkVer {
  implicit val codec: JsonCodec[FlinkVer] = DeriveJsonCodec.gen[FlinkVer]

  def extractMajorVer(flinkVer: String): String = {
    flinkVer.split('.').contra { part =>
      if (part.length < 2) flinkVer
      else part(0) + "." + part(1)
    }
  }
}
