package kce.common

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.util.Timeout
import kce.common.CollectionExtension.IterableWrapper
import kce.common.TimeExtension.FiniteDurationWrapper
import zio.{Schedule, Task, ZIO}

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object ActorExtension {

  implicit class FutureWrapper[Value](future: Future[Value]) {
    @inline def pipeTo[T](ctx: ActorContext[T])(mapResult: Either[Throwable, Value] => T): Unit =
      ctx.pipeToSelf(future)(rs => mapResult(rs.toEither))
  }

  val defaultAskTimeout: FiniteDuration = 2.seconds

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
        .retry(Schedule.recurs(3) && Schedule.spaced(50.millis.asJava))
    } yield actor

  /**
   * ZIO wrapper for Akka actor interoperation.
   */
  implicit class ActorRefWrapper[-T](actor: ActorRef[T]) {

    /**
     * Wrap [[ActorRef.tell]] into ZIO.
     */
    def tellZIO(msg: T): Task[Unit] = ZIO.attempt(actor ! msg)

    /**
     * Wrap [[Askable.ask]] into ZIO.
     */
    def askZIO[Res](replyTo: ActorRef[Res] => T, timeout: Timeout = defaultAskTimeout): ZIO[ActorSystem[_], Throwable, Res] =
      for {
        sys <- ZIO.service[ActorSystem[_]]
        res <- ZIO.fromFuture(implicit ec => actor.ask(replyTo)(timeout, sys.scheduler))
      } yield res

    /**
     * Alias for [[tellZIO]]
     */
    @inline def !>(msg: T): Task[Unit] = tellZIO(msg)

    /**
     * Alias for [[askZIO]]
     */
    @inline def ?>[Res](replyTo: ActorRef[Res] => T, timeout: Timeout = defaultAskTimeout): ZIO[ActorSystem[_], Throwable, Res] =
      askZIO(replyTo, timeout)
  }

}

/**
 * The ActorRef with the specified ServiceKey is not found in Akka Receptionist.
 */
class ActorNotFoundException(message: String) extends Exception(message)

object ActorNotFoundException {
  def apply(serviceKey: ServiceKey[_]): ActorNotFoundException = new ActorNotFoundException(s"ActorRef not found: ${serviceKey.id}")
}
