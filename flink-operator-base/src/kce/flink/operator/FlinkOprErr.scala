package kce.flink.operator

import com.coralogix.zio.k8s.client.K8sFailure
import kce.common.LogMessageTool.LogMessageStringWrapper
import kce.common.{Err, SilentErr}

/**
 * Flink operation error.
 */
sealed abstract class FlinkOprErr(msg: String, cause: Throwable) extends Exception(msg, cause)

case class PodTemplateResolveErr(msg: String, cause: Throwable) extends FlinkOprErr(msg, cause)

case class SubmitFlinkClusterErr(msg: String, cause: Throwable) extends FlinkOprErr(msg, cause)

case class RequestK8sApiErr(msg: String, k8sFailure: K8sFailure) extends FlinkOprErr(msg, Err(k8sFailure.toString))

case class RequestFlinkRestApiErr(msg: String) extends FlinkOprErr(msg, SilentErr)

case class ClusterNotFound(clusterId: String, namespace: String, cause: Throwable = SilentErr)
    extends FlinkOprErr("Flink Cluster not found." tag ("clusterId" -> clusterId, "namespace" -> namespace), cause)

case class SubmitFlinkSessJobErr(msg: String, cause: Throwable) extends FlinkOprErr(msg, cause)

case class NotSupportJobPath(path: String) extends FlinkOprErr(s"Not supported flink job path: $path", SilentErr)

case class ResolveJobGraphErr(msg: String, cause: Throwable) extends FlinkOprErr(msg, cause)
