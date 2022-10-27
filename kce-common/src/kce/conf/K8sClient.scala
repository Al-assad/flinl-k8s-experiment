package kce.conf

import com.coralogix.zio.k8s.client.config.httpclient.k8sSttpClient
import com.coralogix.zio.k8s.client.config.{defaultConfigChain, k8sCluster}
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import com.softwaremill.quicklens.ModifyPimp
import zio.ZLayer

/**
 * ZIO-K8s-Client Layer.
 */
object K8sClient {

  val live: ZLayer[PotaConf, Throwable, Kubernetes] = {
    val configChain = for {
      potaConf <- ZLayer.service[PotaConf]
      config <- defaultConfigChain.update { chain =>
        chain.modify(_.client.debug).setTo(potaConf.get.k8s.debug)
      }
    } yield config
    (configChain >>> (k8sCluster ++ k8sSttpClient)) >>> Kubernetes.live
  }

}
