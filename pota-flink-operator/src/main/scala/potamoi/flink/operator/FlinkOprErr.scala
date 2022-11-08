package potamoi.flink.operator

import com.coralogix.zio.k8s.client.K8sFailure
import potamoi.common.{ActorInteropException, FailProxy, FailStackFill, PotaFail}
import potamoi.flink.observer.FlinkObrErr
import potamoi.flink.share.Fcid
import potamoi.fs.S3Err
import potamoi.k8s
import zio.ZIO

import scala.language.implicitConversions

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

  case class SubmitFlinkSessionClusterErr(fcid: Fcid, cause: Throwable)     extends FlinkOprErr with FailStackFill
  case class SubmitFlinkApplicationClusterErr(fcid: Fcid, cause: Throwable) extends FlinkOprErr with FailStackFill
  case class NotSupportJobJarPath(path: String)                             extends FlinkOprErr
  case class UnableToResolveS3Resource(potaFail: S3Err)                     extends FlinkOprErr with FailProxy

  case class ClusterNotFound(fcid: Fcid)                                extends FlinkOprErr
  case class RequestFlinkRestApiErr(cause: Throwable)                   extends FlinkOprErr with FailStackFill
  case class RequestK8sApiErr(k8sFailure: K8sFailure, cause: Throwable) extends FlinkOprErr with FailStackFill
  case class ActorInteropErr(cause: ActorInteropException)              extends FlinkOprErr with FailStackFill

  object RequestK8sApiErr {
    def apply(k8sFailure: K8sFailure): RequestK8sApiErr = RequestK8sApiErr(k8sFailure, k8s.liftException(k8sFailure).orNull)
  }

  implicit def flattenErr[R, A](zio: ZIO[R, PotaFail, A]): ZIO[R, FlinkOprErr, A] = zio.mapError {
    case e: FlinkOprErr                               => e
    case e: S3Err                                     => UnableToResolveS3Resource(e)
    case FlinkObrErr.ClusterNotFound(fcid)            => ClusterNotFound(fcid)
    case FlinkObrErr.RequestK8sApiErr(failure, cause) => RequestK8sApiErr(failure, cause)
    case FlinkObrErr.RequestFlinkRestApiErr(cause)    => RequestFlinkRestApiErr(cause)
    case FlinkObrErr.ActorInteropErr(cause)           => ActorInteropErr(cause)
  }

}
