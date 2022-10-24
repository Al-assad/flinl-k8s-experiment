package kce.common

import akka.actor.typed.scaladsl.ActorContext
import zio._

/**
 * ZIO extensions for interoperability with Future and Akka.
 */
trait ZIOExtension {

  /**
   * Unsafe running ZIO.
   */
  @inline def zioRun[E, A](zio: IO[E, A]): Exit[E, A] = {
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(zio))
  }

  /**
   * Unsafe running ZIO to Future.
   */
  @inline def zioRunToFuture[E <: Throwable, A](zio: IO[E, A]): CancelableFuture[A] = {
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.runToFuture(zio))
  }

  implicit class IOWrapper[E, A](zio: IO[E, A]) {
    @inline def run: Exit[E, A] = zioRun(zio)
  }

  implicit class TaskWrapper[E <: Throwable, A](zio: IO[E, A]) {

    @inline def runToFuture: CancelableFuture[A] = zioRunToFuture(zio)

    /**
     * Run ZIO to future and pipe to Akka Actor mailbox.
     */
    @inline def runToPipe[T](ctx: ActorContext[T])(mapResult: Either[Throwable, A] => T): Unit = {
      ctx.pipeToSelf(zioRunToFuture(zio))(rs => mapResult(rs.toEither))
    }
  }

  /**
   * Close resource zio.
   */
  def close(resource: AutoCloseable): UIO[Unit] = ZIO.succeed(resource.close())

  /**
   * [[scala.util.Using]] style syntax for ZIO.
   */
  def usingAttempt[RS <: AutoCloseable](code: => RS): ZIO[Any with Scope, Throwable, RS] = ZIO.acquireRelease(ZIO.attempt(code))(close)

  /**
   * Alias for [[ZIO.succeed]]
   */
  @inline def pure[A](a: => A)(implicit trace: Trace): UIO[A] = ZIO.succeed(a)

}

object ZIOExtension extends ZIOExtension
