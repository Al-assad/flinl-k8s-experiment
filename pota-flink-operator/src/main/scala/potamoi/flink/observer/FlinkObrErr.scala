package potamoi.flink.observer

import com.coralogix.zio.k8s.client.K8sFailure
import potamoi.common.{ActorInteropException, FailStackFill, PotaFail}
import potamoi.flink.share.Fcid

/**
 * Flink observer error.
 */
sealed trait FlinkObrErr extends PotaFail

object FlinkObrErr {

  case class ClusterNotFound(fcid: Fcid)                                extends FlinkObrErr
  case class RequestK8sApiErr(k8sFailure: K8sFailure, cause: Throwable) extends FlinkObrErr with FailStackFill
  case class RequestFlinkRestApiErr(cause: Throwable)                       extends FlinkObrErr with FailStackFill
  case class ActorInteropErr(cause: ActorInteropException)              extends FlinkObrErr with FailStackFill

}
