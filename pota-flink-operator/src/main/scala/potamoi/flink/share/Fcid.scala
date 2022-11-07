package potamoi.flink.share

import zio.json.{DeriveJsonCodec, JsonCodec}

import scala.language.implicitConversions

/**
 * Unique flink cluster identifier under the same kubernetes cluster.
 */
case class Fcid(clusterId: String, namespace: String) {
  def toAnno = Array("flink.clusterId" -> clusterId, "flink.namespace" -> namespace)
}

object Fcid {
  implicit val codec: JsonCodec[Fcid]                      = DeriveJsonCodec.gen[Fcid]
  implicit def tuple2ToFcid(tuple: (String, String)): Fcid = Fcid(tuple._1, tuple._2)
}

/**
 * Unique flink job identifier under the same kubernetes cluster.
 */
case class Fjid(clusterId: String, namespace: String, jobId: String) {
  def isUnder(fcid: Fcid): Boolean = fcid.clusterId == clusterId && fcid.namespace == namespace
  def toAnno                       = Array("flink.clusterId" -> clusterId, "flink.namespace" -> namespace, "flink.jobId" -> jobId)
}

object Fjid {
  implicit val codec: JsonCodec[Fjid]        = DeriveJsonCodec.gen[Fjid]
  def apply(fcid: Fcid, jobId: String): Fjid = Fjid(fcid.clusterId, fcid.namespace, jobId)
}
