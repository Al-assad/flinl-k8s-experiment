package potamoi.flink.share

import com.coralogix.zio.k8s.client.K8sFailure
import potamoi.common.{ActorInteropException, FailProxy, FailStackFill, PotaFail}
import potamoi.flink.share.model.Fcid
import potamoi.fs.S3Err
import potamoi.k8s
import potamoi.k8s.K8sErr
import zio.IO

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
  case class EmptyJobInCluster(fcid: Fcid)                                  extends FlinkOprErr
  case class NotSupportJobJarPath(path: String)                             extends FlinkOprErr
  case class UnableToResolveS3Resource(potaFail: S3Err)                     extends FlinkOprErr with FailProxy

  case class ClusterNotFound(fcid: Fcid)        extends FlinkOprErr with PotaFail.NotFound
  case class JobNotFound(jobId: String)         extends FlinkOprErr with PotaFail.NotFound
  case class JarNotFound(jarId: String)         extends FlinkOprErr with PotaFail.NotFound
  case class TriggerNotFound(triggerId: String) extends FlinkOprErr with PotaFail.NotFound
  case class TaskManagerNotFound(tmId: String)  extends FlinkOprErr with PotaFail.NotFound

  case class ActorInteropErr(cause: ActorInteropException)              extends FlinkOprErr with FailStackFill
  case class RequestFlinkRestApiErr(cause: Throwable)                   extends FlinkOprErr with FailStackFill
  case class RequestK8sApiErr(k8sFailure: K8sFailure, cause: Throwable) extends FlinkOprErr with FailStackFill
  case class K8sOperationErr(potaFail: K8sErr)                          extends FlinkOprErr with FailProxy
  case object WatchTimeout                                              extends FlinkOprErr

  object RequestK8sApiErr {
    def apply(k8sFailure: K8sFailure): RequestK8sApiErr = RequestK8sApiErr(k8sFailure, k8s.liftException(k8sFailure).orNull)
  }

  case object IllegalK8sServiceEntity    extends FlinkOprErr
  case object IllegalK8sDeploymentEntity extends FlinkOprErr
  case object IllegalK8sPodEntity        extends FlinkOprErr

  implicit def actorInteropErrConversion[A](io: IO[ActorInteropException, A]): IO[FlinkOprErr, A] = io.mapError(ActorInteropErr)
}
