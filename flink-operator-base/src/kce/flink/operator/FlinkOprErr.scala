package kce.flink.operator

import com.coralogix.zio.k8s.client.K8sFailure
import kce.common.{FailStackFill, PotaFail}
import kce.k8s

/**
 * Flink operation error.
 */
sealed trait FlinkOprErr extends PotaFail

object FlinkOprErr {

  case class IOErr(message: String, cause: Throwable)  extends FlinkOprErr with FailStackFill
  case class ParsePodTemplateYamlErr(cause: Throwable) extends FlinkOprErr with FailStackFill
  case class GenPodTemplateErr(cause: Throwable)       extends FlinkOprErr with FailStackFill

  case class SubmitFlinkSessionClusterErr(clusterId: String, namespace: String, cause: Throwable)     extends FlinkOprErr with FailStackFill
  case class SubmitFlinkApplicationClusterErr(clusterId: String, namespace: String, cause: Throwable) extends FlinkOprErr with FailStackFill

  case class RequestK8sApiErr(k8sFailure: K8sFailure, cause: Throwable) extends FlinkOprErr with FailStackFill
  object RequestK8sApiErr {
    def apply(k8sFailure: K8sFailure): RequestK8sApiErr = RequestK8sApiErr(k8sFailure, k8s.liftException(k8sFailure).orNull)
  }
}
