package kce.common

import kce.common.CollectionExtension.IterableWrapper
import testkit.STSpec

class CollectionExtensionSpec extends STSpec {

  "CollectionExtension" should {

    "get randomEle" in {
      Seq(1, 2, 3).randomEle shouldNot be(None)
      Seq(2).randomEle shouldBe Some(2)
      Seq().randomEle shouldBe None
    }
  }

}
