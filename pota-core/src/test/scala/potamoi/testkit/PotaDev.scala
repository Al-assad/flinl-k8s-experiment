package potamoi.testkit

import potamoi.config._
import potamoi.logger.LogsLevel
import potamoi.syntax.valueToSome
import zio.{ULayer, ZLayer}

import scala.concurrent.duration.DurationInt

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
    ),
    akka = AkkaConf().copy(
      host = "127.0.0.1",
      port = 3300,
      seedsAddress = Set("127.0.0.1:3300")
    ),
    log = LogConf().copy(
      level = LogsLevel.INFO
    ),
    flink = FlinkConf().copy(
      snapshotQuery = FlkSnapshotQueryConf(
        askTimeout = 5.seconds,
        parallelism = 8
      )
    )
  ).resolve

  val conf: ULayer[PotaConf] = ZLayer.succeed(rawConf)

}
