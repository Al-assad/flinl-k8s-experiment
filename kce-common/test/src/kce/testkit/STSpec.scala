package kce.testkit

import kce.common.{PotaFailExtension, ZIOExtension}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

/**
 * Standard test specification.
 */
trait STSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll with ZIOExtension with PotaFailExtension
