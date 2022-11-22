package potamoi.flink.observer

import akka.actor.typed.Behavior
import potamoi.cluster.LWWMapDData
import potamoi.config.DDataConf
import potamoi.flink.share.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.timex._

/**
 * Flink cluster svc rest endpoint distributed data storage base on LWWMap.
 */
private[observer] object RestEptCache extends LWWMapDData[Fcid, FlinkRestSvcEndpoint] {
  val cacheId                               = "flink-rest-endpoint"
  def apply(conf: DDataConf): Behavior[Cmd] = start(conf)()
}
