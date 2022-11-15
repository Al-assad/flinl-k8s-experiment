package potamoi.testkit

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import com.typesafe.config.ConfigFactory

/**
 * Standard test specification for Single-node cluster Akka actor.
 */
trait STActorClusterSpec extends STActorSpec {

  /**
   * Single-node actor cluster config.
   */
  protected val defaultActorConf = ConfigFactory
    .parseString("""akka.actor.provider = cluster
                   |""".stripMargin)

  override def resetActorKit: ActorTestKit = ActorTestKit(defaultActorConf)

}
