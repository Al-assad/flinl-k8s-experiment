package potamoi.k8s

import org.joda.time.DateTime
import potamoi.k8s.K8sErr.DirectRequestK8sApiErr
import potamoi.sttpx._
import sttp.client3._
import zio.ZIO.attempt
import zio.macros.accessible
import zio.{IO, ZIO, ZLayer}

/**
 * Kubernetes operator.
 */
@accessible
trait K8sOperator {

  /**
   * Get Pod metrics info.
   */
  def getPodMetrics(name: String, namespace: String): IO[K8sErr, PodMetrics]

}

object K8sOperator {

  val live: ZLayer[K8sClient, Nothing, Live] = ZLayer {
    for {
      k8sClient <- ZIO.service[K8sClient]
    } yield new Live(k8sClient)
  }

  class Live(k8sClient: K8sClient) extends K8sOperator {

    override def getPodMetrics(name: String, namespace: String): IO[K8sErr, PodMetrics] = {
      k8sClient.usingSttp { (request, backend, host) =>
        request
          .get(uri"$host/apis/metrics.k8s.io/v1beta1/namespaces/$namespace/pods/$name")
          .send(backend)
          .map(_.body)
          .narrowEither
          .flatMap { rsp =>
            attempt {
              val json = ujson.read(rsp)
              val ts   = DateTime.parse(json("timestamp").str).getMillis
              val containers = json("containers").arr.map { container =>
                val name = container("name").str
                val cpu  = QuantityUnit.resolve(container("usage").obj("cpu").str).to(QuantityUnit.m).toLong
                val mem  = QuantityUnit.resolve(container("usage").obj("memory").str).to(QuantityUnit.Ki).toLong
                ContainerMetrics(name, cpu, mem)
              }
              PodMetrics(ts, containers.toVector)
            }
          }
          .mapError(DirectRequestK8sApiErr)
      }
    }
  }

}
