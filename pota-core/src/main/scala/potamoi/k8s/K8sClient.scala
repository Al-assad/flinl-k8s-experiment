package potamoi.k8s

import com.coralogix.zio.k8s.client._
import com.coralogix.zio.k8s.client.config.{defaultConfigChain, httpclient, k8sCluster}
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import com.coralogix.zio.k8s.client.model.K8sCluster
import com.softwaremill.quicklens.ModifyPimp
import potamoi.config.{K8sConf, PotaConf}
import sttp.capabilities.WebSockets
import sttp.capabilities.zio.ZioStreams
import sttp.client3.{Empty, RequestT, SttpBackend, basicRequest}
import sttp.model.Uri
import zio.config.syntax.ZIOConfigNarrowOps
import zio.{Task, ZIO, ZLayer}

import scala.language.implicitConversions

/**
 * ZIO-K8s-Client Layer.
 */
object K8sClient {

  val clive: ZLayer[K8sConf, Throwable, K8sClient] = {
    val k8sClusterConfig = ZLayer.service[K8sConf].flatMap { conf =>
      defaultConfigChain.project(_.modify(_.client.debug).setTo(conf.get.debug))
    }
    val cluster     = k8sClusterConfig >>> k8sCluster
    val sttpBackend = k8sClusterConfig >>> httpclient.k8sSttpClient
    val api         = cluster ++ sttpBackend >>> Kubernetes.live
    cluster.flatMap(c => sttpBackend.flatMap(s => api.flatMap(a => ZLayer.succeed(K8sClient(a.get, c.get, s.get)))))
  }

  val live: ZLayer[PotaConf, Throwable, K8sClient] =
    ZLayer.service[PotaConf].narrow(_.k8s) >>> clive

}

case class K8sClient(api: Kubernetes, cluster: K8sCluster, sttpBackend: SttpBackend[Task, ZioStreams with WebSockets]) {
  def usingSttp[R, E, A](
      f: (RequestT[Empty, Either[String, String], Any], SttpBackend[Task, ZioStreams with WebSockets], Uri) => ZIO[R, E, A]): ZIO[R, E, A] =
    f(basicRequest, sttpBackend, cluster.host)
}
