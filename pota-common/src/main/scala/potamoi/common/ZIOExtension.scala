package potamoi.common

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
  @inline def zioRunToFuture[E, A](zio: IO[E, A]): CancelableFuture[A] = {
    Unsafe.unsafe { implicit u =>
      Runtime.default.unsafe.runToFuture(zio.mapError {
        case e: Throwable => e
        case e: PotaFail  => PotaFailException(e)
        case e            => FutureException[E](e)
      })
    }
  }

  implicit class IOWrapper[E, A](zio: IO[E, A]) {

    /**
     * Sync run zio effect.
     */
    @inline def run: Exit[E, A] = zioRun(zio)

    /**
     * Async run zio effect to scala Future.
     */
    @inline def runToFuture: CancelableFuture[A] = zioRunToFuture(zio)

    /**
     * Run ZIO to future and pipe to Akka Actor mailbox.
     */
    @inline def runToPipe[T](ctx: ActorContext[T])(mapResult: Either[Throwable, A] => T): Unit = {
      ctx.pipeToSelf(zioRunToFuture(zio))(rs => mapResult(rs.toEither))
    }
  }

  implicit class ScopeZIOWrapper[E, A](zio: ZIO[Scope, E, A]) {
    def endScoped(): IO[E, A] = ZIO.scoped(zio)
  }

  /**
   * Close resource zio.
   */
  def close(resource: AutoCloseable): UIO[Unit] = ZIO.succeed(resource.close())

  /**
   * [[scala.util.Using]] style syntax for ZIO.
   */
  def usingAttempt[RS <: AutoCloseable](code: => RS): ZIO[Scope, Throwable, RS] = ZIO.acquireRelease(ZIO.attempt(code))(close)

  /**
   * [[scala.util.Using]] style syntax for ZIO.
   */
  def usingAttemptBlocking[RS <: AutoCloseable](code: => RS): ZIO[Scope, Throwable, RS] =
    ZIO.acquireRelease(ZIO.attemptBlockingInterrupt(code))(close)

}

object ZIOExtension extends ZIOExtension
