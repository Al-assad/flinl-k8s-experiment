package potamoi.flink.operator

import potamoi.common.ScalaVer.Scala212
import potamoi.flink.share.model.FlinkVer
import potamoi.testkit.STSpec

class FlinkPluginsSpec extends STSpec {

  import FlinkPlugins._

  "FlinkPlugins" should {

    "convert plugin simple name to jar name" in {
      s3Hadoop.jarName(FlinkVer("1.15.2", Scala212)) shouldBe "flink-s3-fs-hadoop-1.15.2.jar"
      s3HadoopGS.jarName(FlinkVer("1.15.2", Scala212)) shouldBe "flink-gs-fs-hadoop-1.15.2.jar"
      s3HadoopOSS.jarName(FlinkVer("1.15.2", Scala212)) shouldBe "flink-oss-fs-hadoop-1.15.2.jar"
      s3HadoopAzure.jarName(FlinkVer("1.15.2", Scala212)) shouldBe "flink-azure-fs-hadoop-1.15.2.jar"
      s3Presto.jarName(FlinkVer("1.15.2", Scala212)) shouldBe "flink-s3-fs-presto-1.15.2.jar"
      cep.jarName(FlinkVer("1.15.2", Scala212)) shouldBe "flink-cep_2.12-1.15.2.jar"
      gelly.jarName(FlinkVer("1.15.2", Scala212)) shouldBe "flink-gelly_2.12-1.15.2.jar"
      pyFlink.jarName(FlinkVer("1.15.2", Scala212)) shouldBe "flink-python_2.12-1.15.2.jar"
    }

  }

}
