package potamoi.k8s

import com.coralogix.zio.k8s.client.NotFound
import com.coralogix.zio.k8s.model.apps.v1.DeploymentSpec
import com.coralogix.zio.k8s.model.core.v1.{PodSpec, ServiceSpec}
import org.joda.time.DateTime
import potamoi.k8s.K8sErr._
import potamoi.sttpx._
import sttp.client3._
import zio.ZIO.attempt
import zio.macros.accessible
import zio.prelude.data.Optional.{Absent, Present}
import zio.{IO, ZIO, ZLayer}

/**
 * Kubernetes operator.
 */
@accessible
trait K8sOperator {

  def client: K8sClient

  /**
   * Get pod metrics info.
   */
  def getPodMetrics(name: String, namespace: String): IO[DirectRequestK8sApiErr, PodMetrics]

  /**
   * Get deployment spec.
   */
  def getDeploymentSpec(name: String, namespace: String): IO[K8sErr, DeploymentSpec]

  /**
   * Get service spec.
   */
  def getServiceSpec(name: String, namespace: String): IO[K8sErr, ServiceSpec]

  /**
   * Get pod spec.
   */
  def getPodSpec(name: String, namespace: String): IO[K8sErr, PodSpec]

  /**
   * Get configmaps data.
   */
  def getConfigMapsData(name: String, namespace: String): IO[K8sErr, Map[String, String]]

}

object K8sOperator {
  val live: ZLayer[K8sClient, Nothing, K8sOperatorLive] =
    ZLayer.fromZIO(ZIO.service[K8sClient].map(new K8sOperatorLive(_)))
}

class K8sOperatorLive(k8sClient: K8sClient) extends K8sOperator {

  override def client: K8sClient = k8sClient

  override def getPodMetrics(name: String, namespace: String): IO[DirectRequestK8sApiErr, PodMetrics] =
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
              val cpu  = K8sQuantity(container("usage").obj("cpu").str)
              val mem  = K8sQuantity(container("usage").obj("memory").str)
              ContainerMetrics(name, cpu, mem)
            }
            PodMetrics(ts, containers.toVector)
          }
        }
        .mapError(DirectRequestK8sApiErr)
    }

  override def getDeploymentSpec(name: String, namespace: String): IO[K8sErr, DeploymentSpec] = {
    k8sClient.api.apps.v1.deployments
      .get(name, namespace)
      .flatMap(_.getSpec)
      .mapError {
        case NotFound => DeploymentNotFound(name, namespace)
        case e        => RequestK8sApiErr(e, liftException(e).get)
      }
  }

  override def getServiceSpec(name: String, namespace: String): IO[K8sErr, ServiceSpec] = {
    k8sClient.api.v1.services
      .get(name, namespace)
      .flatMap(_.getSpec)
      .mapError {
        case NotFound => ServiceNotFound(name, namespace)
        case e        => RequestK8sApiErr(e, liftException(e).get)
      }
  }

  override def getPodSpec(name: String, namespace: String): IO[K8sErr, PodSpec] = {
    k8sClient.api.v1.pods
      .get(name, namespace)
      .flatMap(_.getSpec)
      .mapError {
        case NotFound => PodNotFound(name, namespace)
        case e        => RequestK8sApiErr(e, liftException(e).get)
      }
  }

  override def getConfigMapsData(name: String, namespace: String): IO[K8sErr, Map[String, String]] = {
    k8sClient.api.v1.configmaps
      .get(name, namespace)
      .mapBoth(
        {
          case NotFound => ConfigMapNotFound(name, namespace)
          case e        => RequestK8sApiErr(e, liftException(e).get)
        }, {
          _.data match {
            case Present(map) => map
            case Absent       => Map.empty[String, String]
          }
        })
  }

}
