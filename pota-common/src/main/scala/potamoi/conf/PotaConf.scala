package potamoi.conf

import com.softwaremill.quicklens._
import potamoi.common
import potamoi.common.PathTool.rmSlashPrefix
import potamoi.common.{ComplexEnum, GenericPF}
import potamoi.conf.LogsLevel.LogsLevel
import potamoi.conf.LogsStyle.LogsStyle
import potamoi.conf.S3AccessStyle.{PathStyle, S3AccessStyle, VirtualHostedStyle}
import zio.{ULayer, ZIO, ZLayer}

/**
 * Potamoi root configuration.
 */
case class PotaConf(log: LogConf, localStorageDir: String, k8s: K8sConf, s3: S3Conf, flink: FlinkConf) {
  def resolve: PotaConf      = Vector(k8s, s3, flink).foldLeft(this)((a, c) => c.resolve(a))
  def toPrettyString: String = common.toPrettyString(this)
}

object PotaConf {

  val dev: PotaConf = PotaConf(
    log = LogConf(
      level = LogsLevel.INFO,
      style = LogsStyle.Plain,
      colored = true,
      inOneLine = false
    ),
    localStorageDir = "var/potamoi",
    k8s = K8sConf(),
    s3 = S3Conf(
      endpoint = "http://192.168.3.17:30255",
      bucket = "flink-dev",
      accessKey = "minio",
      secretKey = "minio123",
      accessStyle = S3AccessStyle.PathStyle
    ),
    flink = FlinkConf(
      k8sAccount = "flink-opr",
      minioClientImage = "minio/mc:RELEASE.2022-10-12T18-12-50Z",
      localTmpDir = "tmp/flink"
    )
  ).resolve

  val live: ULayer[PotaConf] = ZLayer(ZIO.succeed(PotaConf.dev))

}

sealed trait ResolveConf {
  def resolve: PotaConf => PotaConf = identity
}

/**
 * Logging config.
 */
case class LogConf(level: LogsLevel = LogsLevel.INFO, style: LogsStyle = LogsStyle.Plain, colored: Boolean = true, inOneLine: Boolean = false)
    extends ResolveConf

/**
 * Kubernetes config.
 */
case class K8sConf(debug: Boolean = false) extends ResolveConf

/**
 * S3 storage config.
 */
case class S3Conf(
    endpoint: String,
    bucket: String,
    accessKey: String,
    secretKey: String,
    accessStyle: S3AccessStyle = S3AccessStyle.PathStyle,
    sslEnabled: Boolean = false)
    extends ResolveConf {

  /**
   * Modify s3 path to the correct access style.
   */
  def revisePath(s3Path: String): String = {
    s3Path.split("://").contra {
      case segs if segs.length < 2 => s3Path
      case segs =>
        val revisePathSegs = segs(1).split('/').contra { purePathSegs =>
          accessStyle match {
            case PathStyle          => if (purePathSegs.head == bucket) purePathSegs else Array(bucket) ++ purePathSegs
            case VirtualHostedStyle => if (purePathSegs.head == bucket) purePathSegs.drop(1) else purePathSegs
          }
        }
        segs(0) + "://" + revisePathSegs.mkString("/")
    }
  }
}

/**
 * S3 path access style.
 */
object S3AccessStyle extends ComplexEnum {
  type S3AccessStyle = Value
  val PathStyle          = Value("path-style")
  val VirtualHostedStyle = Value("virtual-hosted-style")
}

/**
 * Flink config.
 */
case class FlinkConf(k8sAccount: String, minioClientImage: String, localTmpDir: String) extends ResolveConf {
  override def resolve = { root =>
    root.modify(_.flink.localTmpDir).using(dir => s"${root.localStorageDir}/${rmSlashPrefix(dir)}")
  }
}
