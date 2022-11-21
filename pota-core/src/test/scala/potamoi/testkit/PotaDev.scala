package potamoi.testkit

import potamoi.config.{DbConf, NodeRole, PotaConf, S3AccessStyle, S3Conf}
import zio.{ULayer, ZLayer}
import potamoi.syntax.valueToSome

object PotaDev {

  val rawConf = PotaConf(
    nodeRoles = Set(NodeRole.Server, NodeRole.FlinkOperator, NodeRole.FlinkSqlInteractor),
    db = DbConf(
      host = "hs.assad.site",
      port = 5432,
      user = "postgres",
      database = "potamoi",
      password = "postgres"
    ),
    s3 = S3Conf(
      endpoint = "http://10.144.74.197:30255",
      bucket = "flink-dev",
      accessKey = "minio",
      secretKey = "minio123",
      accessStyle = S3AccessStyle.PathStyle
    )
  )

  val conf: ULayer[PotaConf] = ZLayer.succeed(rawConf)

}