package potamoi.common

import potamoi.common.PathTool._
import potamoi.testkit.STSpec

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

    "isS3Path" in {
      isS3Path("s3://bucket/xx/xx.jar") shouldBe true
      isS3Path("s3:///bucket/xx/xx.jar") shouldBe true
      isS3Path("s3a://bucket/xx/xx.jar") shouldBe true
      isS3Path("s3n://bucket/xx/xx.jar") shouldBe true
      isS3Path("s3p://bucket/xx/xx.jar") shouldBe true
      isS3Path("hdfs://xx/xx.jar") shouldBe false
      isS3Path("/xx/xx.jar") shouldBe false
      isS3Path("file:///xx/xx.jar") shouldBe false
      isS3Path("") shouldBe false
    }

    "reviseToS3pSchema" in {
      reviseToS3pSchema("s3://b1/file") shouldBe "s3p://b1/file"
      reviseToS3pSchema("s3p://b1/file") shouldBe "s3p://b1/file"
      reviseToS3pSchema("s3a://b1/file") shouldBe "s3p://b1/file"
      reviseToS3pSchema("file:///b1/file") shouldBe "file:///b1/file"
      reviseToS3pSchema("/b1/file") shouldBe "/b1/file"
    }
  }

}
