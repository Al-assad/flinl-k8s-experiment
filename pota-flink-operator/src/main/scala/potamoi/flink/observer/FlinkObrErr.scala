package potamoi.flink.observer

import com.coralogix.zio.k8s.client.K8sFailure
import potamoi.common.ActorExtension.ActorInteropErr
import potamoi.common.{FailStackFill, PotaFail}
import potamoi.flink.operator.FlinkOprErr
import potamoi.flink.share.Fcid

/**
 * Flink observer error.
 */
sealed trait FlinkObrErr extends PotaFail

object FlinkObrErr {

  case class ClusterNotFound(fcid: Fcid)                                extends FlinkObrErr
  case class RequestK8sApiErr(k8sFailure: K8sFailure, cause: Throwable) extends FlinkObrErr with FailStackFill
  case class CacheInteropErr(cause: ActorInteropErr)                    extends FlinkObrErr with FailStackFill

  /**
   * Flatten to [[FlinkOprErr]].
   */
  def flattenFlinkOprErr(err: FlinkObrErr): FlinkOprErr = err match {
    case ClusterNotFound(fcid)               => FlinkOprErr.ClusterNotFound(fcid)
    case RequestK8sApiErr(k8sFailure, cause) => FlinkOprErr.RequestK8sApiErr(k8sFailure, cause)
    case e                                   => FlinkOprErr.UnHandleObserverErr(e)
  }
}
