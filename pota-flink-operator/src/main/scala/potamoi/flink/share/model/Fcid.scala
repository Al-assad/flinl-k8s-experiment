package potamoi.flink.share.model

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
  implicit val ordering: Ordering[Fcid]                    = Ordering.by(fcid => (fcid.clusterId, fcid.namespace))
  implicit def tuple2ToFcid(tuple: (String, String)): Fcid = Fcid(tuple._1, tuple._2)
}

/**
 * Unique flink job identifier under the same kubernetes cluster.
 */
case class Fjid(clusterId: String, namespace: String, jobId: String) {
  def fcid: Fcid                   = Fcid(clusterId, namespace)
  def isUnder(fcid: Fcid): Boolean = fcid.clusterId == clusterId && fcid.namespace == namespace
  def toAnno                       = Array("flink.clusterId" -> clusterId, "flink.namespace" -> namespace, "flink.jobId" -> jobId)
}

object Fjid {
  implicit val codec: JsonCodec[Fjid]        = DeriveJsonCodec.gen[Fjid]
  implicit val ordering: Ordering[Fjid]      = Ordering.by(fjid => (fjid.clusterId, fjid.namespace, fjid.jobId))
  def apply(fcid: Fcid, jobId: String): Fjid = Fjid(fcid.clusterId, fcid.namespace, jobId)
}

/**
 * Unique flink taskmanager identifier under the same kubernetes cluster.
 */
case class Ftid(clusterId: String, namespace: String, tid: String) {
  def fcid: Fcid          = Fcid(clusterId, namespace)
  def isUnder(fcid: Fcid) = fcid.clusterId == clusterId && fcid.namespace == namespace
}
object Ftid {
  implicit val codec: JsonCodec[Ftid]       = DeriveJsonCodec.gen[Ftid]
  implicit val ordering: Ordering[Ftid]     = Ordering.by(ftid => (ftid.clusterId, ftid.namespace, ftid.tid))
  def apply(fcid: Fcid, tmId: String): Ftid = Ftid(fcid.clusterId, fcid.namespace, tmId)
}
