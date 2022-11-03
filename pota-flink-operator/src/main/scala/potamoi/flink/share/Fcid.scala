package potamoi.flink.share

import zio.json.{DeriveJsonCodec, JsonCodec}

import scala.language.implicitConversions

/**
 * Flink kubernetes cluster id with namespace.
 */
case class Fcid(clusterId: String, namespace: String) {

  def toAnno = Array("flink.clusterId" -> clusterId, "flink.namespace" -> namespace)
}

object Fcid {
  implicit val codec: JsonCodec[Fcid]                      = DeriveJsonCodec.gen[Fcid]
  implicit def tuple2ToFcid(tuple: (String, String)): Fcid = Fcid(tuple._1, tuple._2)
}
