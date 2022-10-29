package potamoi.conf

import potamoi.conf.S3AccessStyle.{PathStyle, VirtualHostedStyle}
import potamoi.testkit.STSpec

class PotaConfSpec extends STSpec {

  "S3Conf" should {

    "revisePath to path style" in {
      val s3Conf = S3Conf(
        endpoint = "http://s3.assad.site",
        bucket = "b1",
        accessKey = "114514",
        secretKey = "114514",
        accessStyle = PathStyle
      )
      s3Conf.revisePath("s3://b1/test/file") shouldBe "s3://b1/test/file"
      s3Conf.revisePath("s3://test/file") shouldBe "s3://b1/test/file"
    }

    "revisePath to virtual-hosted style" in {
      val s3Conf = S3Conf(
        endpoint = "http://b1.s3.assad.site",
        bucket = "b1",
        accessKey = "114514",
        secretKey = "114514",
        accessStyle = VirtualHostedStyle
      )
      s3Conf.revisePath("s3://b1/test/file") shouldBe "s3://test/file"
      s3Conf.revisePath("s3://test/file") shouldBe "s3://test/file"
    }
  }

}
