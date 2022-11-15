package potamoi.k8s

import com.coralogix.zio.k8s.client.config.{defaultConfigChain, httpclient, k8sCluster}
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import com.softwaremill.quicklens.ModifyPimp
import potamoi.conf.{K8sConf, PotaConf}
import zio.{ZIO, ZLayer}

/**
 * ZIO-K8s-Client Layer.
 */
object K8sClient {

  val live2: ZLayer[K8sConf, Throwable, Kubernetes] = {
    val configChain = for {
      conf <- ZLayer.service[K8sConf]
      config <- defaultConfigChain.update { chain =>
        chain.modify(_.client.debug).setTo(conf.get.debug)
      }
    } yield config
    (configChain >>> (k8sCluster ++ httpclient.k8sSttpClient)) >>> Kubernetes.live
  }

  val live: ZLayer[PotaConf, Throwable, Kubernetes] = {
    ZLayer(ZIO.service[PotaConf].map(_.k8s)) >>> live2
  }

}
