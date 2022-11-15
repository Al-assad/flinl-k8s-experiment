package potamoi.common

import potamoi.common.CollectionExtension.IterableWrapper
import potamoi.testkit.STSpec

class CollectionExtensionSpec extends STSpec {

  "CollectionExtension" should {

    "get randomEle" in {
      Seq(1, 2, 3).randomEle shouldNot be(None)
      Seq(2).randomEle shouldBe Some(2)
      Seq().randomEle shouldBe None
    }
  }

}
