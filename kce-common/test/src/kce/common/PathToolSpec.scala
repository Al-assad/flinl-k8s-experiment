package kce.common

import kce.common.PathTool._
import kce.testkit.STSpec

class PathToolSpec extends STSpec {

  "PathTool" should {

    "purePath" in {
      purePath("s3://bucket/xx/xx.jar") shouldBe "bucket/xx/xx.jar"
      purePath("s3:///bucket/xx/xx.jar") shouldBe "bucket/xx/xx.jar"
      purePath("/bucket/xx/xx.jar") shouldBe "bucket/xx/xx.jar"
    }

    "rmSlashPrefix" in {
      rmSlashPrefix("/xx/xx.jar") shouldBe "xx/xx.jar"
      rmSlashPrefix("xx/xx.jar") shouldBe "xx/xx.jar"
      rmSlashPrefix("") shouldBe ""
    }
  }

}
