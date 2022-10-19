package kce.conf

import com.coralogix.zio.k8s.client.apps.v1.deployments.Deployments
import com.coralogix.zio.k8s.client.config.httpclient.k8sDefault
import com.coralogix.zio.k8s.client.v1.pods.Pods
import com.coralogix.zio.k8s.client.v1.services.Services

object KceConf {

  val default: KceConf = KceConf(
    localStoDir = "kce",
    k8s = K8sConf(
      flinkAccount = "flink-opr"
    ),
    s3 = S3Conf(
      endpoint = "http://192.168.3.17:30255",
      accessKey = "minio",
      secretKey = "minio123"
    )
  )
}

case class KceConf(localStoDir: String, k8s: K8sConf, s3: S3Conf) {
  lazy val flinkLogConfDir = s"$localStoDir/flink/logconf"
}

case class K8sConf(flinkAccount: String)

case class S3Conf(endpoint: String, accessKey: String, secretKey: String, pathStyleAccess: Boolean = true, sslEnabled: Boolean = false)

object K8sLayer {
  val live = k8sDefault >>> (Services.live ++ Pods.live ++ Deployments.live)
}
