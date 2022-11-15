package potamoi.k8s

import com.coralogix.zio.k8s.client.config.{defaultConfigChain, httpclient, k8sCluster}
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import com.softwaremill.quicklens.ModifyPimp
import potamoi.conf.{K8sConf, PotaConf}
import zio.ZLayer
import zio.config.syntax.ZIOConfigNarrowOps

/**
 * ZIO-K8s-Client Layer.
 */
object K8sClient {

  val live: ZLayer[PotaConf, Throwable, Kubernetes] = ZLayer.service[PotaConf].narrow(_.k8s) >>> clive

  val clive: ZLayer[K8sConf, Throwable, Kubernetes] = ZLayer.service[K8sConf].flatMap { conf =>
    val configChain = defaultConfigChain.update { chain =>
      chain.modify(_.client.debug).setTo(conf.get.debug)
    }
    (configChain >>> (k8sCluster ++ httpclient.k8sSttpClient)) >>> Kubernetes.live
  }

}
