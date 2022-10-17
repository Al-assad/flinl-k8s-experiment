package kce.common

import akka.actor.typed.scaladsl.ActorContext
import zio._

object ZIOExtension {

  @inline def zioRun[E, A](zio: IO[E, A]): Exit[E, A] = {
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(zio))
  }

  @inline def zioRunToFuture[E <: Throwable, A](zio: IO[E, A]): CancelableFuture[A] = {
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.runToFuture(zio))
  }

  implicit class IOWrapper[E, A](zio: IO[E, A]) {
    @inline def run: Exit[E, A] = zioRun(zio)
  }

  implicit class TaskWrapper[E <: Throwable, A](zio: IO[E, A]) {
    @inline def runToFuture: CancelableFuture[A] = zioRunToFuture(zio)

    @inline def runToPipe[T](ctx: ActorContext[T])(mapResult: Either[Throwable, A] => T): Unit =
      ctx.pipeToSelf(zioRunToFuture(zio))(rs => mapResult(rs.toEither))
  }

  def close(resource: AutoCloseable): UIO[Unit] = ZIO.succeed(resource.close())

}
