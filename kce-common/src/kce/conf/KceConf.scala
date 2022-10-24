package kce.conf
import com.softwaremill.quicklens._
import kce.common.PathTool.rmSlashPrefix
import zio.{ULayer, ZIO, ZLayer}

/**
 * Potamoi root configuration.
 */
case class KceConf(localStorageDir: String, k8s: K8sConf, s3: S3Conf, flink: FlinkConf) {
  def resolve: KceConf = Vector(k8s, s3, flink).foldLeft(this)((a, c) => c.resolve(a))
}

// TODO initFs

object KceConf {
  val default: KceConf = KceConf(
    localStorageDir = "kce",
    k8s = K8sConf(),
    s3 = S3Conf(
      endpoint = "http://192.168.3.17:30255",
      accessKey = "minio",
      secretKey = "minio123"
    ),
    flink = FlinkConf(
      k8sAccount = "flink-opr",
      minioClientImage = "minio/mc:RELEASE.2022-10-12T18-12-50Z",
      logConfDir = "flink/logconf",
      localTmpDir = "flink/tmp"
    )
  ).resolve

  val live: ULayer[KceConf] = ZLayer(ZIO.succeed(KceConf.default))
}

sealed trait ResolveConf {
  def resolve: KceConf => KceConf = identity
}

/**
 * Kubernetes config.
 */
case class K8sConf(debug: Boolean = false) extends ResolveConf

/**
 * S3 storage config.
 */
case class S3Conf(endpoint: String, accessKey: String, secretKey: String, pathStyleAccess: Boolean = true, sslEnabled: Boolean = false)
    extends ResolveConf

/**
 * Flink config.
 */
case class FlinkConf(k8sAccount: String, minioClientImage: String, logConfDir: String, localTmpDir: String) extends ResolveConf {
  override def resolve = { root =>
    root
      .modify(_.flink.logConfDir)
      .using(dir => s"${root.localStorageDir}/${rmSlashPrefix(dir)}")
      .modify(_.flink.localTmpDir)
      .using(dir => s"${root.localStorageDir}/${rmSlashPrefix(dir)}")
  }
}
