package potamoi.config

import com.softwaremill.quicklens.ModifyPimp
import potamoi.common.ComplexEnum
import potamoi.config.FlkRestEndpointType.FlkRestEndpointType
import potamoi.{common, pathx}
import zio.config.magnolia.name
import zio.json.{DeriveJsonCodec, JsonCodec}

import scala.concurrent.duration.{Duration, DurationInt}

/**
 * Flink module configuration.
 */
case class FlinkConf(
    @name("k8s-account") k8sAccount: String = "flink-opr",
    @name("mc-image") minioClientImage: String = "minio/mc:RELEASE.2022-10-12T18-12-50Z",
    @name("local-tmpdir") localTmpDir: String = "tmp/flink",
    @name("rest-endpoint-internal") restEndpointTypeInternal: FlkRestEndpointType = FlkRestEndpointType.ClusterIp,
    @name("query-ask-timeout") queryAskTimeout: Duration = 60.seconds,
    @name("tracking") tracking: FlkTrackConf = FlkTrackConf())
    extends Resolvable {

  override def resolve = { root =>
    root
      .modify(_.flink.localTmpDir)
      .using(dir => s"${root.localStgDir}/${pathx.rmSlashPrefix(dir)}")
  }
}

object FlinkConf {
  implicit val codec: JsonCodec[FlinkConf] = DeriveJsonCodec.gen[FlinkConf]
}

/**
 * Flink tracking config.
 */
case class FlkTrackConf(
    @name("poll-job") jobPolling: Duration = 500.millis,
    @name("poll-savepoint-trigger") savepointTriggerPolling: Duration = 100.millis)

object FlkTrackConf {
  implicit val durCodec: JsonCodec[Duration]  = common.scalaDurationCodec
  implicit val codec: JsonCodec[FlkTrackConf] = DeriveJsonCodec.gen[FlkTrackConf]
}

/**
 * Flink rest api export type.
 */
object FlkRestEndpointType extends ComplexEnum {
  type FlkRestEndpointType = Value
  val SvcDns    = Value("svc-dns")
  val ClusterIp = Value("cluster-ip")
}
