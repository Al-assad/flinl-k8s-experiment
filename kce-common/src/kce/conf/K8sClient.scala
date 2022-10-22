package kce.conf

import com.coralogix.zio.k8s.client.config.httpclient
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import zio.ZLayer

object K8sClient {
  val live: ZLayer[Any, Throwable, Kubernetes] = httpclient.k8sDefault >>> Kubernetes.live
}
