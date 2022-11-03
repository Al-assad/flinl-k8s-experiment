package potamoi

import org.scalatest.Tag
import org.scalatest.exceptions.TestFailedException
import potamoi.common.ZIOExtension.zioRun
import zio.Exit.{Failure, Success}
import zio.{Cause, FiberFailure, IO}

package object testkit {

  /**
   * Test suit only enabled for local dev environment.
   */
  object UnsafeEnv extends Tag("unsafeEnv")

  /**
   * Run zio synchronously in spec, returning the effect channel
   * and throwing the error channel as an FiberFailure exception
   * but throwing the assertion exception in ZIO effect as is.
   */
  def zioRunInSpec[E, A](zio: IO[E, A]): A = zioRun(zio) match {
    case Success(value) => value
    case Failure(cause) =>
      cause match {
        case Cause.Fail(e, _) =>
          e match {
            case e: TestFailedException => throw e
            case _                      => throw FiberFailure(cause)
          }
        case c => throw FiberFailure(c)
      }
  }

}
