package potamoi.testkit

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import potamoi.common.{PotaFailExtension, ZIOExtension}

/**
 * Standard test specification.
 */
trait STSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll with ZIOExtension with PotaFailExtension
