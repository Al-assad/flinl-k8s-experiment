package potamoi.flink.observer

import com.coralogix.zio.k8s.client.K8sFailure
import potamoi.common.{FailStackFill, PotaFail}
import potamoi.flink.operator.FlinkOprErr
import potamoi.flink.share.Fcid
import potamoi.k8s

/**
 * Flink observer error.
 */
sealed trait FlinkObrErr extends PotaFail

object FlinkObrErr {

  case class ClusterNotFound(fcid: Fcid) extends FlinkObrErr

  case class RequestK8sApiErr(k8sFailure: K8sFailure, cause: Throwable) extends FlinkObrErr with FailStackFill
  object RequestK8sApiErr {
    def apply(k8sFailure: K8sFailure): RequestK8sApiErr = RequestK8sApiErr(k8sFailure, k8s.liftException(k8sFailure).orNull)
  }

  /**
   * Flatten to [[FlinkOprErr]].
   */
  def flattenFlinkOprErr(err: FlinkObrErr): FlinkOprErr = err match {
    case ClusterNotFound(fcid)               => FlinkOprErr.ClusterNotFound(fcid)
    case RequestK8sApiErr(k8sFailure, cause) => FlinkOprErr.RequestK8sApiErr(k8sFailure, cause)
    case e                                   => FlinkOprErr.UnHandleObserverErr(e)
  }
}
