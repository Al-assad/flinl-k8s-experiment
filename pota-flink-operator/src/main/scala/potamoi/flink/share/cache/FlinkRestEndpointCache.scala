package potamoi.flink.share.cache

import akka.actor.typed.Behavior
import potamoi.cluster.LWWMapDData
import potamoi.config.DDataConf
import potamoi.flink.share.model.{Fcid, FlinkRestSvcEndpoint}

/**
 * Flink cluster svc rest endpoint distributed data storage base on LWWMap.
 */
object FlinkRestEndpointCache extends LWWMapDData[Fcid, FlinkRestSvcEndpoint] {
  val cacheId                               = "flink-rest-endpoint"
  def apply(conf: DDataConf): Behavior[Cmd] = start(conf)
}



