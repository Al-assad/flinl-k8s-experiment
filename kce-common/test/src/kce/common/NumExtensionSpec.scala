package kce.common

import kce.testkit.STSpec

class NumExtensionSpec extends STSpec {

  "int extension" should {
    import kce.common.NumExtension.IntWrapper

    "ensureMin" in {
      10.ensureMin(5) shouldBe 10
      10.ensureMin(10) shouldBe 10
      10.ensureMin(15) shouldBe 15
    }
    "ensureMoreThan" in {
      10.ensureOr(_ > 5, 0) shouldBe 10
      10.ensureOr(_ > 10, 0) shouldBe 0
      10.ensureOr(_ > 15, 0) shouldBe 0
    }
  }

  "double extension" should {
    import kce.common.NumExtension.DoubleWrapper

    "ensureMin" in {
      10.0.ensureMin(5.0) shouldBe 10.0
      10.0.ensureMin(10.0) shouldBe 10.0
      10.0.ensureMin(15.0) shouldBe 15.0
    }
    "ensureMoreThan" in {
      10.0.ensureOr(_ > 5.0, 0.0) shouldBe 10.0
      10.0.ensureOr(_ > 10.0, 0.0) shouldBe 0.0
      10.0.ensureOr(_ > 15.0, 0.0) shouldBe 0.0
    }
  }

}
