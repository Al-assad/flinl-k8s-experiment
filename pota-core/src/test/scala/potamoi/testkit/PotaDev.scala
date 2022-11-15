package potamoi.testkit

import potamoi.conf.{NodeRole, PotaConf, S3AccessStyle, S3Conf}
import zio.ZLayer

object PotaDev {

  val conf = PotaConf(
    nodeRoles = Set(NodeRole.Server, NodeRole.FlinkOperator, NodeRole.FlinkSqlInteractor),
    s3 = S3Conf(
      endpoint = "http://10.144.74.197:30255",
      bucket = "flink-dev",
      accessKey = "minio",
      secretKey = "minio123",
      accessStyle = S3AccessStyle.PathStyle
    )
  )

  val confLayer = ZLayer.succeed(conf)

}
