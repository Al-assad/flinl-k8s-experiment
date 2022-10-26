package kce.fs

import kce.common.MimeType
import kce.conf.KceConf
import kce.testkit.STSpec

class S3OperatorSpec extends STSpec {

  // TODO unsafe
  "s3 operator" should {
    "download" in {
      S3Operator
        .download("s3://flink-dev/flink-connector-jdbc-1.15.2.jar", "spec-test/flink-connector-jdbc-1.15.2.jar")
        .provide(KceConf.live, S3Operator.live)
        .debugErr
        .run
    }
    "upload" in {
      S3Operator
        .upload("spec-test/flink-connector-jdbc-1.15.2.jar", "s3://flink-dev/flink-connector-jdbc-1.15.2@re.jar", MimeType.jar)
        .provide(KceConf.live, S3Operator.live)
        .debugErr
        .run
    }
    "remove" in {
      S3Operator
        .remove("s3://flink-dev/flink-connector-jdbc-1.15.2@re.jar")
        .provide(KceConf.live, S3Operator.live)
        .debugErr
        .run
    }
    "exists" in {
      S3Operator
        .exists("s3://flink-dev/flink-connector-jdbc-1.15.2@re.jar")
        .provide(KceConf.live, S3Operator.live)
        .debugErr
        .run
      S3Operator
        .exists("s3://flink-dev/flink-connector-jdbc-1.15.2.jar")
        .provide(KceConf.live, S3Operator.live)
        .debugErr
        .run
    }
  }

  override protected def afterAll(): Unit = lfs.rm("spec-test")
}
