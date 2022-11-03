package potamoi.testkit

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorSystem
import com.typesafe.config.ConfigFactory
import potamoi.common.ActorExtension
import potamoi.conf.PotaLogger
import zio.{ZIO, ZLayer}

/**
 * Standard test specification for Akka Actor.
 */
trait STActorSpec extends STSpec with ActorExtension {

  /**
   * Customize [[ActorTestKit]] for this specification.
   */
  def resetActorKit: ActorTestKit = ActorTestKit()

  lazy val actorKit: ActorTestKit                                      = resetActorKit
  lazy val actorSys: ActorSystem[Nothing]                              = actorKit.system
  lazy val actorSysLayer: ZLayer[Any, Throwable, ActorSystem[Nothing]] = ZLayer(ZIO.attempt(actorKit.system))

  override def afterAll(): Unit = {
    super.afterAll()
    actorKit.shutdownTestKit()
  }

  implicit class ActorSpecZIORunner[E, A](zio: ZIO[ActorSystem[Nothing], E, A]) {
    def runActorSpec: A = zioRunInSpec {
      if (enableLog) zio.provideLayer(actorSysLayer ++ PotaLogger.logLayer())
      else zio.provideLayer(actorSysLayer)
    }
  }
}

/**
 * Standard test specification for Single-node cluster Akka actor.
 */
trait STActorClusterSpec extends STActorSpec {

  /**
   * Single-node actor cluster config.
   */
  protected val defaultActorConf = ConfigFactory.parseString("akka.actor.provider=cluster")

  override def resetActorKit: ActorTestKit = ActorTestKit(defaultActorConf)

}
