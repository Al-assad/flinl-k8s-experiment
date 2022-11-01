package potamoi.testkit

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import potamoi.common.{PotaFailExtension, ZIOExtension}
import potamoi.conf.PotaLogger
import zio.{Exit, IO, ULayer}

/**
 * Standard test specification.
 */
trait STSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll with ZIOExtension with PotaFailExtension {

  implicit class DebugZIOWrapper[E, A](zio: IO[E, A]) {

    /**
     * Sync run zio effect on debug mode.
     */
    def runDebug: Exit[E, A] = zio.debug.provideLayer(PotaLogger.debug).run

    /**
     * Sync run zio effect on debug mode with more options.
     */
    def runDebug(dumpFailStack: Boolean = false, log: ULayer[Unit] = PotaLogger.debug): Exit[E, A] = {
      if (dumpFailStack) zio.debugStack.provideLayer(log).run
      else zio.debug.provideLayer(log).run
    }
  }

}
