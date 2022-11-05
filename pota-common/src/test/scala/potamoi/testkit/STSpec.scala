package potamoi.testkit

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import potamoi.PotaLogger
import potamoi.common.{PotaFailExtension, ZIOExtension}
import zio._

/**
 * Standard test specification.
 */
trait STSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll with ZIOExtension with PotaFailExtension {

  /**
   * Whether to enable logging
   */
  protected def enableLog: Boolean = true

  /**
   * Throwable wrapper for ZIO normal Cause.
   */
  case class ZIOCause[E](err: E) extends Throwable

  /**
   * Run ZIO effect.
   */
  implicit class SpecZIORunner[E, A](zio: IO[E, A]) {
    def runSpec: A = zioRunInSpec(if (enableLog) zio.provideLayer(PotaLogger.layer()) else zio)
  }

}
