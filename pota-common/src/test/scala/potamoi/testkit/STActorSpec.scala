package potamoi.testkit

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorSystem
import potamoi.common.ActorExtension
import zio.{ZIO, ZLayer}

/**
 * Standard test specification for Akka Actor.
 */
trait STActorSpec extends STSpec with ActorExtension {

  def provideTestKit: ActorTestKit = ActorTestKit()

  val actorKit: ActorTestKit = provideTestKit

  val actorSys: ActorSystem[Nothing] = actorKit.system

  val actorSysLayer: ZLayer[Any, Throwable, ActorSystem[Nothing]] = ZLayer(ZIO.attempt(actorKit.system))

  override def afterAll(): Unit = actorKit.shutdownTestKit()

}
