package potamoi.testkit

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, Props, Scheduler}
import com.typesafe.config.ConfigFactory
import potamoi.PotaLogger
import potamoi.common.ActorExtension
import zio.{Task, ZIO, ZLayer}

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
  implicit lazy val sc: Scheduler                                      = actorSys.scheduler
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

  /**
   * Spawn an actor with ZIO effect.
   */
  def spawn[T](behavior: Behavior[T], props: Props = Props.empty): Task[ActorRef[T]] = ZIO.attempt(actorKit.spawn(behavior, props))

  /**
   * Stop an actor with ZIO effect.
   */
  def stop[T](ref: ActorRef[T]): Task[Unit] = ZIO.attempt(actorKit.stop(ref))
}

/**
 * Standard test specification for Single-node cluster Akka actor.
 */
trait STActorClusterSpec extends STActorSpec {

  /**
   * Single-node actor cluster config.
   */
  protected val defaultActorConf = ConfigFactory
    .parseString("""akka.actor.provider = cluster
                   |akka.log-dead-letters-during-shutdown = false
                   |""".stripMargin)

  override def resetActorKit: ActorTestKit = ActorTestKit(defaultActorConf)

}
