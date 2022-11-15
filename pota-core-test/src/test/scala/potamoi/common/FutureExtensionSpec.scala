package potamoi.common

import potamoi.common.FutureExtension.FutureWrapper
import potamoi.testkit.STSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class FutureExtensionSpec extends STSpec {

  "FutureExtension" should {

    "asZIO" in {
      Future(1 + 1).asZIO.runSpec shouldBe 2
    }

    "blocking result" in {
      Future(1 + 1).blocking() shouldBe 2
    }

    "safeBlocking result" in {
      case object Boom extends Exception
      Future(throw Boom).safeBlocking() shouldBe Left(Boom)
    }

  }
}
