package kce.common

import kce.testkit.STSpec
import S3Tool._

class S3ToolSpec extends STSpec {

  "S3Tool" should {
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
  }

}
