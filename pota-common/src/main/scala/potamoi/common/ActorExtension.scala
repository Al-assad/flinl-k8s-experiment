package potamoi.common

import akka.actor.typed._
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.util.Timeout
import potamoi.common.CollectionExtension.IterableWrapper
import zio.{IO, Schedule, UIO, ZIO}
import potamoi.timex._

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Akka Extension for interoperability with ZIO and Future.
 */
trait ActorExtension {

  implicit class FutureWrapper[Value](future: Future[Value]) {
    @inline def pipeTo[T](ctx: ActorContext[T])(mapResult: Either[Throwable, Value] => T): Unit =
      ctx.pipeToSelf(future)(rs => mapResult(rs.toEither))
  }

  val defaultAskTimeout: FiniteDuration = 5.seconds

  /**
   * Find ActorRef from ActorSystem via ServiceKey.
   */
  def findActor[T](serviceKey: ServiceKey[T], askTimeout: Timeout = defaultAskTimeout): ZIO[ActorSystem[_], Throwable, ActorRef[T]] =
    for {
      system <- ZIO.service[ActorSystem[_]]
      actor <- ZIO
        .fromFuture(implicit ec => system.receptionist.ask(Receptionist.Find(serviceKey))(askTimeout, system.scheduler))
        .map(_.serviceInstances(serviceKey).randomEle)
        .flatMap {
          case Some(actor) => ZIO.succeed(actor)
          case None        => ZIO.fail(ActorNotFoundException(serviceKey))
        }
        .retry(Schedule.recurs(3) && Schedule.spaced(50.millis))
    } yield actor

  /**
   * ZIO wrapper for Akka actor interoperation.
   */
  implicit class ActorRefWrapper[-T](actor: ActorRef[T]) {

    /**
     * Wrap [[ActorRef.tell]] into ZIO.
     */
    def tellZIO(msg: T): IO[ActorInteropException, Unit] = ZIO.attempt(actor ! msg).mapError(ActorInteropException)

    /**
     * Wrap [[Askable.ask]] into ZIO.
     */
    def askZIO[Res](replyTo: ActorRef[Res] => T)(implicit sc: Scheduler, askTimeout: Timeout = defaultAskTimeout): IO[ActorInteropException, Res] = {
      ZIO.fromFuture(implicit ec => actor.ask(replyTo)(askTimeout, sc)).mapError(ActorInteropException)
    }

    /**
     * Alias for [[tellZIO]]
     */
    def !>(msg: T): IO[ActorInteropException, Unit] = ZIO.attempt(actor ! msg).mapError(ActorInteropException)

    /**
     * Wrapping actor tell behavior via zio and ignoring all side effects.
     */
    def !!>(msg: T): UIO[Unit] = ZIO.attempt(actor ! msg).ignore

    /**
     * Alias for [[askZIO]]
     */
    def ?>[Res](replyTo: ActorRef[Res] => T)(implicit sc: Scheduler, askTimeout: Timeout = defaultAskTimeout): IO[ActorInteropException, Res] =
      ZIO.fromFuture(implicit ec => actor.ask(replyTo)(askTimeout, sc)).mapError(ActorInteropException)
  }

  /**
   * Actor Behavior enhancement.
   */
  implicit class BehaviorWrapper[T](behavior: Behavior[T]) {

    /**
     * Behaviors.supervise.onFailure
     */
    def onFailure[Thr <: Throwable](strategy: SupervisorStrategy)(implicit tag: ClassTag[Thr] = ClassTag(classOf[Throwable])): Behavior[T] = {
      Behaviors.supervise(behavior).onFailure[Thr](strategy)
    }

    /**
     * Execute the function before the behavior begins.
     */
    def beforeIt(func: => Unit): Behavior[T] = {
      func; behavior
    }
  }

}

object ActorExtension extends ActorExtension

/**
 * Actor interoperation error.
 */
case class ActorInteropException(cause: Throwable) extends Exception(cause)

/**
 * The ActorRef with the specified ServiceKey is not found in Akka Receptionist.
 */
class ActorNotFoundException(message: String) extends Exception(message)

object ActorNotFoundException {
  def apply(serviceKey: ServiceKey[_]): ActorNotFoundException = new ActorNotFoundException(s"ActorRef not found: ${serviceKey.id}")
}
