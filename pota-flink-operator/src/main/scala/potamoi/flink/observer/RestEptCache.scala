package potamoi.flink.observer

import akka.actor.typed.Behavior
import akka.cluster.ddata.typed.scaladsl.Replicator.{ReadLocal, WriteLocal}
import potamoi.cluster.LWWMapDData
import potamoi.conf.AkkaConf
import potamoi.flink.share.{Fcid, FlinkRestSvcEndpoint}

/**
 * Flink cluster svc rest endpoint distributed data storage base on LWWMap.
 */
private[observer] object RestEptCache extends LWWMapDData[Fcid, FlinkRestSvcEndpoint] {

  val cacheId    = "flink-rest-endpoint"
  val writeLevel = WriteLocal
  val readLevel  = ReadLocal

  def apply(akkaConf: AkkaConf): Behavior[Cmd] = start(akkaConf.ddata.askTimeout)()
}
