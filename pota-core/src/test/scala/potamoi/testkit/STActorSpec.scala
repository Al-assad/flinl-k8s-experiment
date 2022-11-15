package potamoi.testkit

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed._
import akka.util.Timeout
import org.scalatest.time.Span
import potamoi.common.ActorExtension
import potamoi.logger.PotaLogger
import zio.{Task, ZIO, ZLayer}

import scala.language.implicitConversions

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
      if (enableLog) zio.provideLayer(actorSysLayer ++ PotaLogger.layer())
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

  implicit def spanToTimeout(span: Span): Timeout = Timeout(Span.convertSpanToDuration(span))
}
