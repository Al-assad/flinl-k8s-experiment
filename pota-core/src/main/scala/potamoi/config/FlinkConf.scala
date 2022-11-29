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
    @name("snapshot-query") snapshotQuery: FlkSnapshotQueryConf = FlkSnapshotQueryConf(),
    @name("tracking") tracking: FlkTrackConf = FlkTrackConf())
    extends Resolvable {

  override def resolve = { root =>
    root
      .modify(_.flink.localTmpDir)
      .using(dir => s"${root.localStgDir}/${pathx.rmSlashPrefix(dir)}")
  }
}

object FlinkConf {
  implicit val durCodec: JsonCodec[Duration]                         = common.scalaDurationCodec
  implicit val trackConfCodec: JsonCodec[FlkTrackConf]               = DeriveJsonCodec.gen[FlkTrackConf]
  implicit val snapshotQryConfCodec: JsonCodec[FlkSnapshotQueryConf] = DeriveJsonCodec.gen[FlkSnapshotQueryConf]
  implicit val codec: JsonCodec[FlinkConf]                           = DeriveJsonCodec.gen[FlinkConf]
}

/**
 * Flink cluster tracking config.
 */
case class FlkTrackConf(
    @name("poll-job-overview") jobOvPolling: Duration = 500.millis,
    @name("poll-cluster-overview") clusterOvPolling: Duration = 500.millis,
    @name("poll-tm-detail") tmdDetailPolling: Duration = 2.seconds,
    @name("poll-jm-metrics") jmMetricsPolling: Duration = 5.seconds,
    @name("poll-tm-metrics") tmMetricsPolling: Duration = 5.seconds,
    @name("poll-job-metrics") jobMetricsPolling: Duration = 2.seconds,
    @name("poll-savepoint-trigger") savepointTriggerPolling: Duration = 100.millis)

/**
 * Configuration of Flink snapshot queries stored in Akka distributed
 * system (DData or cluster-sharding).
 */
case class FlkSnapshotQueryConf(
    @name("timeout") askTimeout: Duration = 60.seconds,
    @name("parallelism") parallelism: Int = 16)

/**
 * Flink rest api export type.
 */
object FlkRestEndpointType extends ComplexEnum {
  type FlkRestEndpointType = Value
  val SvcDns    = Value("svc-dns")
  val ClusterIp = Value("cluster-ip")
}
