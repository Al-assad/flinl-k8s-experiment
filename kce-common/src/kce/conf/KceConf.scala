package kce.conf
import com.softwaremill.quicklens._
import kce.common.PathTool.rmSlashPrefix

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
      localLogConfDir = "flink/logconf"
    )
  ).resolve
}

case class KceConf(localStorageDir: String, k8s: K8sConf, s3: S3Conf, flink: FlinkConf) {
  def resolve: KceConf = Vector(k8s, s3, flink).foldLeft(this)((a, c) => c.resolve(a))
}

sealed trait ResolveConf {
  def resolve: KceConf => KceConf = identity
}

/**
 * Kubernetes config.
 */
case class K8sConf() extends ResolveConf

/**
 * S3 storage config.
 */
case class S3Conf(endpoint: String, accessKey: String, secretKey: String, pathStyleAccess: Boolean = true, sslEnabled: Boolean = false)
    extends ResolveConf

/**
 * Flink config.
 */
case class FlinkConf(k8sAccount: String, minioClientImage: String, localLogConfDir: String) extends ResolveConf {
  override def resolve = { root =>
    root.modify(_.flink.localLogConfDir).using(dir => s"${root.localStorageDir}/${rmSlashPrefix(dir)}")
  }
}
