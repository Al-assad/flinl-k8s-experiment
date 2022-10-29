package potamoi.fs

import potamoi.common.MimeType
import potamoi.conf.PotaConf
import potamoi.testkit.STSpec

class S3OperatorSpec extends STSpec {

  // TODO unsafe
  "s3 operator" should {
    "download" in {
      S3Operator
        .download("s3://flink-dev/flink-connector-jdbc-1.15.2.jar", "spec-test/flink-connector-jdbc-1.15.2.jar")
        .provide(PotaConf.live, S3Operator.live)
        .debugStack
        .run
    }
    "upload" in {
      S3Operator
        .upload("spec-test/flink-connector-jdbc-1.15.2.jar", "s3://flink-dev/flink-connector-jdbc-1.15.2@re.jar", MimeType.jar)
        .provide(PotaConf.live, S3Operator.live)
        .debugStack
        .run
    }
    "remove" in {
      S3Operator
        .remove("s3://flink-dev/flink-connector-jdbc-1.15.2@re.jar")
        .provide(PotaConf.live, S3Operator.live)
        .debugStack
        .run
    }
    "exists" in {
      S3Operator
        .exists("s3://flink-dev/flink-connector-jdbc-1.15.2@re.jar")
        .provide(PotaConf.live, S3Operator.live)
        .debugStack
        .run
      S3Operator
        .exists("s3://flink-dev/flink-connector-jdbc-1.15.2.jar")
        .provide(PotaConf.live, S3Operator.live)
        .debugStack
        .run
    }
  }

  override protected def afterAll(): Unit = lfs.rm("spec-test")
}
