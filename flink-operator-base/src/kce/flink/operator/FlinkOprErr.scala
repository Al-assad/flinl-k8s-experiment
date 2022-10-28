package kce.flink.operator

import com.coralogix.zio.k8s.client.K8sFailure
import kce.common.{FailProxy, FailStackFill, PotaFail}
import kce.fs.S3Err
import kce.k8s

/**
 * Flink operation error.
 */
sealed trait FlinkOprErr extends PotaFail

object FlinkOprErr {

  case class IOErr(message: String, cause: Throwable)   extends FlinkOprErr with FailStackFill
  case class DecodePodTemplateYamlErr(cause: Throwable) extends FlinkOprErr with FailStackFill
  case class EncodePodTemplateYamlErr(cause: Throwable) extends FlinkOprErr with FailStackFill
  case class GenPodTemplateErr(cause: Throwable)        extends FlinkOprErr with FailStackFill

  case class ReviseFlinkClusterDefErr(cause: Throwable) extends FlinkOprErr with FailStackFill
  case class DryToFlinkRawConfigErr(cause: Throwable)   extends FlinkOprErr with FailStackFill

  case class SubmitFlinkSessionClusterErr(clusterId: String, namespace: String, cause: Throwable)     extends FlinkOprErr with FailStackFill
  case class SubmitFlinkApplicationClusterErr(clusterId: String, namespace: String, cause: Throwable) extends FlinkOprErr with FailStackFill
  case class NotSupportJobJarPath(path: String)                                                       extends FlinkOprErr
  case class UnableToResolveS3Resource(potaFail: S3Err)                                               extends FlinkOprErr with FailProxy
  case class ClusterNotFound(clusterId: String, namespace: String)                                    extends FlinkOprErr

  case class RequestFlinkRestApiErr(message: String) extends FlinkOprErr

  case class RequestK8sApiErr(k8sFailure: K8sFailure, cause: Throwable) extends FlinkOprErr with FailStackFill
  object RequestK8sApiErr {
    def apply(k8sFailure: K8sFailure): RequestK8sApiErr = RequestK8sApiErr(k8sFailure, k8s.liftException(k8sFailure).orNull)
  }

}
