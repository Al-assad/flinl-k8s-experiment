package potamoi.flink.share.cache

import akka.actor.typed.Behavior
import potamoi.cluster.ORSetDData
import potamoi.config.DDataConf
import potamoi.flink.share.model.Fcid

/**
 * Enable flink cluster rest proxy set base on ORSet.
 */
object FlinkRestProxyCache extends ORSetDData[Fcid] {
  val cacheId: String                       = "flink-rest-proxy"
  def apply(conf: DDataConf): Behavior[Cmd] = start(conf)
}

