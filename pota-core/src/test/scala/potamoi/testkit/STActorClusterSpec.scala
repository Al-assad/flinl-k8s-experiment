package potamoi.testkit

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.DurationInt

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

  implicit val askTimeout: Timeout = 5.seconds

  override def resetActorKit: ActorTestKit = ActorTestKit(defaultActorConf)

}
