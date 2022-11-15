package potamoi.common

import potamoi.testkit.STSpec

class NumExtensionSpec extends STSpec {

  "int extension" should {
    import potamoi.common.NumExtension.IntWrapper

    "ensureMin" in {
      10.ensureIntMin(5) shouldBe 10
      10.ensureIntMin(10) shouldBe 10
      10.ensureIntMin(15) shouldBe 15
    }
    "ensureMoreThan" in {
      10.ensureIntOr(_ > 5, 0) shouldBe 10
      10.ensureIntOr(_ > 10, 0) shouldBe 0
      10.ensureIntOr(_ > 15, 0) shouldBe 0
    }
  }

  "double extension" should {
    import potamoi.common.NumExtension.DoubleWrapper

    "ensureMin" in {
      10.0.ensureDoubleMin(5.0) shouldBe 10.0
      10.0.ensureDoubleMin(10.0) shouldBe 10.0
      10.0.ensureDoubleMin(15.0) shouldBe 15.0
    }
    "ensureMoreThan" in {
      10.0.ensureDoubleOr(_ > 5.0, 0.0) shouldBe 10.0
      10.0.ensureDoubleOr(_ > 10.0, 0.0) shouldBe 0.0
      10.0.ensureDoubleOr(_ > 15.0, 0.0) shouldBe 0.0
    }
  }

}
