package potamoi.k8s

import com.coralogix.zio.k8s.client.K8sFailure
import potamoi.common.{FailStackFill, PotaFail}

/**
 * K8s operation failure.
 */
trait K8sErr extends PotaFail

object K8sErr {

  case class RequestK8sApiErr(k8sFailure: K8sFailure, cause: Throwable) extends K8sErr with FailStackFill
  case class DirectRequestK8sApiErr(cause: Throwable)                   extends K8sErr with FailStackFill

  case class DeploymentNotFound(name: String, namespace: String) extends K8sErr
  case class ServiceNotFound(name: String, namespace: String)    extends K8sErr
  case class PodNotFound(name: String, namespace: String)        extends K8sErr

}
